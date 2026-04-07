from io import TextIOWrapper
from abc import abstractmethod
from pathlib import Path
from typing import Any, Tuple, override

import gc
import logging
import os
import time

import numpy as np
import pandas as pd

from .quality_control import check_genotype_count, validate_biallelic

logger = logging.getLogger(__name__)


class InvalidIlluminaReportError(Exception):
    pass


class Converter:
    @abstractmethod
    def convert(self, data: pd.DataFrame, *args, **kwargs) -> Any:
        pass


class IlluminaReportConverter(Converter):
    """Read genotype data and convert it to PLINK .map/.ped.

    Supports two input formats:

    1. **Illumina Final Report** (long format) — one row per SNP per sample,
       with ``[Header]`` / ``[Data]`` sections.  Read via
       :meth:`read_illumina_report`.

    2. **Wide format** — one row per SNP, one column per individual
       (tab-delimited).  Read via :meth:`read_wide_format`.

    Both readers produce a normalised *long-format*
    :class:`~pandas.DataFrame` that is fed into :meth:`convert`, which
    pivots it into PLINK MAP / PED output.

    For convenience, :meth:`convert_file` reads + converts + writes in one
    call.
    """

    # Column name mapping — keys are internal names, values are expected
    # column headers in the Illumina report.  Override via constructor.
    DEFAULT_COLUMN_MAP: dict[str, str] = {
        'snp_name': 'SNP Name',
        'sample_id': 'Sample ID',
        'allele1': 'Allele1 - Top',
        'allele2': 'Allele2 - Top',
        'chromosome': 'Chr',
        'position': 'Position',
    }

    data_headers: tuple[str, ...] = ()
    report_headers: dict[str, str] = {}
    required_data_headers: list[str] = []

    def __init__(
        self,
        required_data_headers: Tuple[str, ...] = (),
        column_map: dict[str, str] | None = None,
        missing_genotype: str = '-',
        min_genotype_count: int = 0,
        sex: int = 3,
        phenotype: int = -9,
    ) -> None:
        """Initialise the converter.

        Args:
            required_data_headers: Extra columns that must be present
                (in addition to the column-map values).
            column_map: Override default Illumina column-name mapping.
                Keys: ``snp_name``, ``sample_id``, ``allele1``, ``allele2``,
                ``chromosome``, ``position``.
            missing_genotype: The allele value that represents a missing call.
                For Illumina reports the default is ``'-'``; for wide-format
                files it is typically ``'NN'`` (pass via constructor).
            min_genotype_count: Genotypes appearing ``<=`` this many times
                across all individuals for a SNP are set to missing.
                ``0`` disables the filter.
            sex: Default sex code for PED
                (``1`` = male, ``2`` = female, ``3`` = unknown).
            phenotype: Default phenotype value for PED (``-9`` = missing).
        """
        super().__init__()
        self.required_data_headers = list(required_data_headers)
        self.column_map = dict(self.DEFAULT_COLUMN_MAP)
        if column_map:
            self.column_map.update(column_map)
        self.missing_genotype = missing_genotype
        self.min_genotype_count = min_genotype_count
        self.default_sex = sex
        self.default_phenotype = phenotype

    # ==================================================================
    #  Input readers — both return a normalised long-format DataFrame
    #  with columns matching self.column_map values.
    # ==================================================================

    # ---- 1. Illumina Final Report ------------------------------------

    def read_illumina_report(self, filepath: Path) -> pd.DataFrame:
        """Read an Illumina Final Report file and return a DataFrame.

        The file is expected to have a ``[Header]`` section followed by a
        ``[Data]`` section.  The first row after ``[Data]`` contains column
        names; subsequent rows contain data.

        The returned DataFrame is in *long format* (one row per
        SNP × sample) and can be passed directly to :meth:`convert`.
        """
        with open(filepath, 'r', encoding='utf-8') as f:
            nextline = 0
            while True:
                line = f.readline()
                nextline += 1
                if line == '':
                    raise InvalidIlluminaReportError(
                        'Reached end of file without finding [Header].'
                    )
                if line.startswith('[Header]'):
                    break

            self.report_headers, nextline = self._read_report_headers(f, nextline)
            self.data_headers, nextline = self._read_data_headers(f, nextline)

        col = self.column_map
        needed = set(col.values())
        dtype_map: dict[str, str] = {}
        for h in self.data_headers:
            if h in needed:
                if h in (col['snp_name'], col['sample_id'],
                         col['allele1'], col['allele2']):
                    dtype_map[h] = 'category'
                else:
                    dtype_map[h] = 'str'

        usecols = [h for h in self.data_headers if h in needed]

        df = pd.read_csv(
            filepath,
            sep='\t',
            skiprows=nextline,
            names=list(self.data_headers),
            usecols=usecols,
            dtype=dtype_map,
        )
        logger.info(
            'Read Illumina report: %d rows, %d columns.', len(df), len(df.columns)
        )
        return df

    # ---- 2. Wide format (SNP-major) ----------------------------------

    def read_wide_format(self, filepath: Path) -> pd.DataFrame:
        """Read a wide-format genotype file and return a long-format DataFrame.

        Expected input (tab-delimited)::

            SNP_id  chr  position  Individual1  Individual2  ...
            rs123   1    12345     AA           AG           ...

        Each genotype cell is a 2-character string (e.g. ``AA``, ``AG``,
        ``NN``).  The method *melts* the wide table into the same
        long-format layout that :meth:`read_illumina_report` produces, so
        the result can be passed directly to :meth:`convert`.
        """
        col = self.column_map
        snp_col = col['snp_name']
        sample_col = col['sample_id']
        a1_col = col['allele1']
        a2_col = col['allele2']
        chr_col = col['chromosome']
        pos_col = col['position']

        # Use columnar lists instead of list-of-dicts to reduce memory
        snp_names: list[str] = []
        sample_ids: list[str] = []
        allele1s: list[str] = []
        allele2s: list[str] = []
        chromosomes: list[str] = []
        positions: list[str] = []

        with open(filepath, 'r', encoding='utf-8') as fh:
            header_line = fh.readline().rstrip('\n')
            header = header_line.split('\t')
            individuals = header[3:]

            if not individuals:
                raise ValueError(
                    'No individual columns found in the header. '
                    'Expected: SNP_id\\tchr\\tposition\\tInd1\\tInd2\\t…'
                )

            for line_num, raw_line in enumerate(fh, start=2):
                line = raw_line.rstrip('\n')
                if not line:
                    continue
                parts = line.split('\t')
                if len(parts) < 4:
                    logger.warning('Skipping malformed line %d.', line_num)
                    continue

                snp_id = parts[0]
                chromosome = parts[1]
                position = parts[2]
                genotypes = parts[3:]

                if len(genotypes) != len(individuals):
                    raise ValueError(
                        f'Line {line_num}: expected {len(individuals)} '
                        f'genotypes, got {len(genotypes)}.'
                    )

                for ind_id, geno in zip(individuals, genotypes):
                    if len(geno) == 2:
                        a1, a2 = geno[0], geno[1]
                    else:
                        a1, a2 = self.missing_genotype, self.missing_genotype
                    snp_names.append(snp_id)
                    sample_ids.append(ind_id)
                    allele1s.append(a1)
                    allele2s.append(a2)
                    chromosomes.append(chromosome)
                    positions.append(position)

        # Build DataFrame with categorical dtypes for repeated strings
        df = pd.DataFrame({
            snp_col: pd.Categorical(snp_names),
            sample_col: pd.Categorical(sample_ids),
            a1_col: pd.Categorical(allele1s),
            a2_col: pd.Categorical(allele2s),
            chr_col: chromosomes,
            pos_col: positions,
        })
        # Free the raw lists
        del snp_names, sample_ids, allele1s, allele2s, chromosomes, positions

        logger.info(
            'Read wide-format file: %d rows (long), %d columns.',
            len(df),
            len(df.columns),
        )
        return df

    # ---- 3. Direct wide-format → geno (no long-format round-trip) ----

    def _convert_wide_to_geno_file(
        self,
        input_path: Path,
        output_path: Path,
    ) -> Path:
        """Convert a wide-format file directly to geno.txt.

        Avoids the expensive wide → long → wide round-trip by reading the
        wide file, performing QC, sorting by chromosome/position, and
        writing the geno.txt output in a single streaming pass.

        Returns:
            Path to the written geno.txt file.
        """
        col = self.column_map
        missing_geno = self.missing_genotype + self.missing_genotype

        with open(input_path, 'r', encoding='utf-8') as fh:
            header_line = fh.readline().rstrip('\n')
            header_parts = header_line.split('\t')
            individuals = header_parts[3:]
            n_ind = len(individuals)

            if not individuals:
                raise ValueError(
                    'No individual columns found in the header. '
                    'Expected: SNP_id\\tchr\\tposition\\tInd1\\tInd2\\t…'
                )

            # Collect SNP rows: (chr_int, pos_int, snp_id, genotypes_list)
            snp_rows: list[tuple[int, int, str, list[str]]] = []

            for line_num, raw_line in enumerate(fh, start=2):
                line = raw_line.rstrip('\n')
                if not line:
                    continue
                parts = line.split('\t')
                if len(parts) < 4:
                    logger.warning('Skipping malformed line %d.', line_num)
                    continue

                snp_id = parts[0]
                chr_str = parts[1]
                pos_str = parts[2]
                genotypes = parts[3:]

                if len(genotypes) != n_ind:
                    raise ValueError(
                        f'Line {line_num}: expected {n_ind} '
                        f'genotypes, got {len(genotypes)}.'
                    )

                # Filter to numeric chromosomes 1–29
                try:
                    c_int = int(chr_str)
                    p_int = int(pos_str)
                except (ValueError, TypeError):
                    continue
                if not (1 <= c_int <= 29):
                    continue

                # Normalise genotypes: 2-char or missing
                normalised: list[str] = []
                for geno in genotypes:
                    if len(geno) == 2:
                        if geno == missing_geno or self.missing_genotype in geno:
                            normalised.append('NN')
                        else:
                            normalised.append(geno)
                    else:
                        normalised.append('NN')

                # QC
                normalised = check_genotype_count(
                    normalised, 'NN', snp_id, self.min_genotype_count,
                )
                validate_biallelic(
                    normalised, 'NN', snp_id, line_num,
                )

                snp_rows.append((c_int, p_int, snp_id, normalised))

        logger.info(
            'Read wide-format: %d SNPs retained (chr 1–29), %d individuals.',
            len(snp_rows), n_ind,
        )

        # Sort by chromosome then position
        snp_rows.sort(key=lambda x: (x[0], x[1]))

        # Write geno.txt
        samples_header = '\t'.join(individuals)
        with open(output_path, 'w', encoding='utf-8', newline='\n') as fh:
            fh.write(f'snp\tchr\tpos\t{samples_header}\n')
            for c_int, p_int, snp_id, genos in snp_rows:
                geno_str = '\t'.join(genos)
                fh.write(f'{snp_id}\t{c_int}\t{p_int}\t{geno_str}\n')

        del snp_rows
        gc.collect()

        return output_path

    # ==================================================================
    #  Convert: normalised long-format DataFrame → PLINK MAP / PED
    # ==================================================================

    @override
    def convert(
        self,
        data: pd.DataFrame,
        *args,
        **kwargs,
    ) -> dict[str, list[str]]:
        """Convert long-format genotype DataFrame to PLINK map/ped lines.

        Args:
            data: Long-format DataFrame as returned by
                :meth:`read_illumina_report` or :meth:`read_wide_format`.

        Returns:
            ``{'map': [...], 'ped': [...], 'individuals': [...]}``
            where ``map`` and ``ped`` are lists of tab-delimited strings
            ready to be written line-by-line.
        """
        col = self.column_map
        required = [
            col['snp_name'],
            col['sample_id'],
            col['allele1'],
            col['allele2'],
            col['chromosome'],
            col['position'],
        ]
        missing = [c for c in required if c not in data.columns]
        if missing:
            raise InvalidIlluminaReportError(
                f'Missing required columns: {missing}. '
                f'Available: {list(data.columns)}'
            )

        snp_col = col['snp_name']
        sample_col = col['sample_id']
        a1_col = col['allele1']
        a2_col = col['allele2']
        chr_col = col['chromosome']
        pos_col = col['position']

        # --- Unique SNPs (preserve file order) ---
        snp_info = (
            data[[snp_col, chr_col, pos_col]]
            .drop_duplicates(subset=[snp_col])
            .reset_index(drop=True)
        )
        snp_order: list[str] = snp_info[snp_col].tolist()
        n_snps = len(snp_order)

        # --- Unique individuals (preserve file order) ---
        individuals: list[str] = list(dict.fromkeys(data[sample_col].tolist()))
        n_ind = len(individuals)
        logger.info('Found %d SNPs, %d individuals.', n_snps, n_ind)

        # --- Build MAP lines ---
        map_lines: list[str] = []
        for _, row in snp_info.iterrows():
            chrom = str(row[chr_col])
            if chrom == 'PseudoX':
                chrom = 'XY'
            map_lines.append(f'{chrom}\t{row[snp_col]}\t0\t{row[pos_col]}')

        # --- Build genotype Series without copying the whole DataFrame ---
        geno_series = data[a1_col].astype(str) + data[a2_col].astype(str)

        # --- QC per SNP ---
        snp_to_index = {s: i for i, s in enumerate(snp_order)}
        ind_to_index = {s: i for i, s in enumerate(individuals)}
        missing_geno = self.missing_genotype + self.missing_genotype

        # Pre-allocate numpy arrays: allele1[ind_idx, snp_idx], allele2[...]
        allele1_mat = np.full((n_ind, n_snps), '0', dtype='U1')
        allele2_mat = np.full((n_ind, n_snps), '0', dtype='U1')

        # Group by SNP for QC and filling
        grouped = data.groupby(snp_col, sort=False)
        for snp_name, grp_idx in grouped.groups.items():
            snp_idx = snp_to_index[snp_name]
            geno_list = geno_series.iloc[grp_idx].tolist()
            sample_ids = data[sample_col].iloc[grp_idx].tolist()

            # QC: check genotype counts
            geno_list = check_genotype_count(
                geno_list,
                missing_geno,
                str(snp_name),
                self.min_genotype_count,
            )

            # Validate biallelic
            validate_biallelic(
                geno_list, missing_geno, str(snp_name), snp_idx + 1
            )

            for sample_id, geno in zip(sample_ids, geno_list):
                ind_idx = ind_to_index[sample_id]
                if geno == missing_geno or self.missing_genotype in geno:
                    pass  # already '0'
                else:
                    if len(geno) != 2:
                        raise ValueError(
                            f'Unexpected genotype \'{geno}\' for SNP '
                            f'\'{snp_name}\', sample \'{sample_id}\'.'
                        )
                    allele1_mat[ind_idx, snp_idx] = geno[0]
                    allele2_mat[ind_idx, snp_idx] = geno[1]

        del geno_series, grouped
        gc.collect()

        # --- Build PED lines ---
        sex = str(self.default_sex)
        phe = str(self.default_phenotype)
        ped_lines: list[str] = []
        for i, ind_id in enumerate(individuals):
            parts: list[str] = []
            for j in range(n_snps):
                parts.append(allele1_mat[i, j])
                parts.append(allele2_mat[i, j])
            geno_str = '\t'.join(parts)
            ped_lines.append(
                f'{ind_id}\t{ind_id}\t0\t0\t{sex}\t{phe}\t{geno_str}'
            )

        del allele1_mat, allele2_mat
        gc.collect()

        return {
            'map': map_lines,
            'ped': ped_lines,
            'individuals': individuals,
        }

    # ==================================================================
    #  Convert: normalised long-format DataFrame → geno.txt
    # ==================================================================

    def convert_to_geno(
        self,
        data: pd.DataFrame,
        *args,
        **kwargs,
    ) -> dict[str, Any]:
        """Convert long-format genotype DataFrame to geno.txt format.

        Produces a SNP-major wide table: one row per SNP, one column per
        individual.  SNPs are sorted by chromosome (numeric, 1–29) then
        by position.  Non-numeric chromosomes are excluded.

        Args:
            data: Long-format DataFrame as returned by
                :meth:`read_illumina_report` or :meth:`read_wide_format`.

        Returns:
            ``{'header': str, 'lines': [...], 'individuals': [...]}``
            where ``header`` is the header line and ``lines`` are
            tab-delimited data rows ready to be written line-by-line.
        """
        col = self.column_map
        required = [
            col['snp_name'],
            col['sample_id'],
            col['allele1'],
            col['allele2'],
            col['chromosome'],
            col['position'],
        ]
        missing = [c for c in required if c not in data.columns]
        if missing:
            raise InvalidIlluminaReportError(
                f'Missing required columns: {missing}. '
                f'Available: {list(data.columns)}'
            )

        snp_col = col['snp_name']
        sample_col = col['sample_id']
        a1_col = col['allele1']
        a2_col = col['allele2']
        chr_col = col['chromosome']
        pos_col = col['position']

        missing_geno = self.missing_genotype + self.missing_genotype

        # --- Unique individuals (preserve file order) ---
        individuals: list[str] = list(dict.fromkeys(data[sample_col].tolist()))
        n_ind = len(individuals)
        ind_to_index = {s: i for i, s in enumerate(individuals)}

        # --- Unique SNPs with chr/pos info ---
        snp_info = (
            data[[snp_col, chr_col, pos_col]]
            .drop_duplicates(subset=[snp_col])
            .reset_index(drop=True)
        )
        n_snps = len(snp_info)
        logger.info('Found %d SNPs, %d individuals.', n_snps, n_ind)

        # --- Pre-allocate SNP-major matrix using numpy (much lower memory
        #     than list[list[str]]) ---
        snp_order: list[str] = snp_info[snp_col].tolist()
        snp_to_index = {s: i for i, s in enumerate(snp_order)}
        geno_matrix = np.full((n_snps, n_ind), 'NN', dtype='U2')

        # --- Build genotype Series without copying the whole DataFrame ---
        geno_series = data[a1_col].astype(str) + data[a2_col].astype(str)

        # --- QC per SNP and fill matrix ---
        grouped = data.groupby(snp_col, sort=False)
        for snp_name, grp_idx in grouped.groups.items():
            snp_idx = snp_to_index[snp_name]
            geno_list = geno_series.iloc[grp_idx].tolist()
            sample_ids = data[sample_col].iloc[grp_idx].tolist()

            # QC: check genotype counts
            geno_list = check_genotype_count(
                geno_list,
                missing_geno,
                str(snp_name),
                self.min_genotype_count,
            )

            # Validate biallelic
            validate_biallelic(
                geno_list, missing_geno, str(snp_name), snp_idx + 1
            )

            for sample_id, geno in zip(sample_ids, geno_list):
                ind_idx = ind_to_index[sample_id]
                if geno == missing_geno or self.missing_genotype in geno:
                    pass  # already 'NN'
                else:
                    geno_matrix[snp_idx, ind_idx] = geno

        # Free the temporary Series
        del geno_series, grouped
        gc.collect()

        # --- Build sorted output lines (chr 1–29, then by position) ---
        # Attach chr/pos to each SNP row for sorting
        snp_rows: list[tuple[int, int, str, int]] = []  # (chr, pos, snp_name, snp_idx)
        for idx, row in snp_info.iterrows():
            try:
                c_int = int(row[chr_col])
                p_int = int(row[pos_col])
            except (ValueError, TypeError):
                continue  # Skip non-numeric chromosomes
        ## TODO: Extend this for other animals than cattle
            if not (1 <= c_int <= 29):
                continue
            snp_rows.append((c_int, p_int, row[snp_col], int(idx)))

        snp_rows.sort(key=lambda x: (x[0], x[1]))

        geno_lines: list[str] = []
        for c_int, p_int, snp_name, snp_idx in snp_rows:
            genos = '\t'.join(geno_matrix[snp_idx])
            geno_lines.append(f'{snp_name}\t{c_int}\t{p_int}\t{genos}')

        # Free the numpy matrix
        del geno_matrix
        gc.collect()

        # --- Header ---
        samples_header = '\t'.join(individuals)
        header = f'snp\tchr\tpos\t{samples_header}'

        logger.info(
            'Geno conversion: %d SNPs retained (chr 1–29), %d individuals.',
            len(geno_lines),
            n_ind,
        )

        return {
            'header': header,
            'lines': geno_lines,
            'individuals': individuals,
        }

    # ==================================================================
    #  Convenience: read + convert + write in one call
    # ==================================================================

    def convert_file(
        self,
        input_file: str | os.PathLike,
        output_prefix: str | os.PathLike,
    ) -> dict[str, Path]:
        """Convert a geno.txt file to PLINK .map / .ped files.

        The input is a geno.txt file (produced by :meth:`convert_geno_file`)
        in wide format::

            snp    chr  pos  Sample1  Sample2  …
            rs123  1    100  AA       AG       …

        Missing genotypes are represented as ``NN``.

        Output files (derived from *output_prefix*)::

            <prefix>.map    — PLINK MAP file
            <prefix>.ped    — PLINK PED file
            <prefix>_id.txt — one individual ID per line

        Args:
            input_file: Path to the geno.txt file.
            output_prefix: Base path for output files.

        Returns:
            ``{'map': Path, 'ped': Path, 'id': Path}`` pointing to the
            written output files.
        """
        start = time.perf_counter()
        input_path = Path(input_file)
        prefix = Path(output_prefix)

        map_path = prefix.with_suffix('.map')
        ped_path = prefix.with_suffix('.ped')
        id_path = Path(f'{prefix}_id.txt')

        missing_geno = 'NN'# geno.txt always uses NN for missing
        sex = str(self.default_sex)
        phe = str(self.default_phenotype)

        # --- First pass: count SNPs to pre-allocate numpy array ---
        with open(input_path, 'r', encoding='utf-8') as in_f:
            header = in_f.readline().strip().split('\t')
            individuals = header[3:]  # skip snp, chr, pos
            n_ind = len(individuals)
            snp_count = sum(1 for line in in_f if line.strip())
        logger.info('Found %d individuals, %d SNPs.', n_ind, snp_count)

        # --- Pre-allocate numpy arrays (much less memory than dict of lists) ---
        # Store allele pairs as two U1 arrays: shape (n_ind, snp_count)
        allele1_mat = np.full((n_ind, snp_count), '0', dtype='U1')
        allele2_mat = np.full((n_ind, snp_count), '0', dtype='U1')

        map_lines: list[str] = []

        # --- Second pass: fill matrix ---
        with open(input_path, 'r', encoding='utf-8') as in_f:
            in_f.readline()  # skip header
            snp_idx = 0
            for raw_line in in_f:
                line = raw_line.strip()
                if not line:
                    continue
                parts = line.split('\t')
                snp_id, chr_num, pos = parts[0], parts[1], parts[2]
                genotypes = parts[3:]

                # --- Biallelic validation (warn and continue) ---
                allele_check: set[str] = set()
                for i, geno in enumerate(genotypes):
                    if geno != missing_geno:
                        a1, a2 = geno[0], geno[1]
                        allele1_mat[i, snp_idx] = a1
                        allele2_mat[i, snp_idx] = a2
                        allele_check.add(a1)
                        allele_check.add(a2)

                if len(allele_check) > 2:
                    logger.warning(
                        "SNP '%s' has more than 2 alleles: %s",
                        snp_id,
                        sorted(allele_check),
                    )

                # --- MAP line ---
                chr_label = 'XY' if chr_num == 'PseudoX' else chr_num
                map_lines.append(f'{chr_label}\t{snp_id}\t0\t{pos}')
                snp_idx += 1

        logger.info('Processed %d SNPs.', snp_count)

        # --- Write MAP ---
        self._write_lines(map_lines, map_path, label='MAP')
        del map_lines

        # --- Write PED (stream to disk — one individual at a time) ---
        with open(ped_path, 'w', encoding='utf-8', newline='\n') as fh:
            for i, ind in enumerate(individuals):
                # Build genotype string from numpy rows
                parts_list: list[str] = []
                a1_row = allele1_mat[i]
                a2_row = allele2_mat[i]
                for j in range(snp_count):
                    parts_list.append(a1_row[j])
                    parts_list.append(a2_row[j])
                geno_str = '\t'.join(parts_list)
                fh.write(
                    f'{ind}\t{ind}\t0\t0\t{sex}\t{phe}\t{geno_str}\n'
                )
        logger.info('Wrote PED file (%d lines): %s', n_ind, ped_path)
        del allele1_mat, allele2_mat
        gc.collect()

        # --- Write individual IDs ---
        self._write_ids(individuals, id_path)

        elapsed = time.perf_counter() - start
        logger.info('File conversion completed in %.2f seconds.', elapsed)
        logger.info('MAP: %s  (%d SNPs)', map_path, snp_count)
        logger.info('PED: %s  (%d individuals)', ped_path, n_ind)

        return {
            'map': map_path,
            'ped': ped_path,
            'id': id_path,
        }


    def convert_geno_file(
        self,
        input_file: str | os.PathLike,
        output_file: str | os.PathLike,
        input_format: str = 'illumina',
    ) -> Path:
        """Read a final report text file containing genotype data and write a geno.txt file.

        The output is a SNP-major wide table sorted by chromosome (1–29)
        then position::

            snp    chr  pos  Sample1  Sample2  …
            rs123  1    100  AA       AG       …

        Args:
            input_file: Path to the input genotype file.
            output_file: Path for the output geno.txt file.
            input_format: ``'illumina'`` for Illumina Final Report or
                ``'wide'`` for wide-format (SNP-major) files.

        Returns:
            :class:`~pathlib.Path` to the written geno file.
        """
        start = time.perf_counter()
        input_path = Path(input_file)
        output_path = Path(output_file).with_suffix('.geno.txt')

        # Read and convert
        if input_format == 'illumina':
            df = self.read_illumina_report(input_path)
            # Convert — then free the DataFrame immediately
            result = self.convert_to_geno(df)
            del df
            gc.collect()

            # Write — stream lines to disk
            n_lines = len(result['lines'])
            n_individuals = len(result['individuals'])
            with open(output_path, 'w', encoding='utf-8', newline='\n') as fh:
                fh.write(result['header'] + '\n')
                for line in result['lines']:
                    fh.write(line + '\n')
            del result
            gc.collect()
        elif input_format == 'wide':
            # Direct wide → geno path: avoids the expensive
            # wide → long-format DataFrame → wide round-trip.
            self._convert_wide_to_geno_file(input_path, output_path)
            n_lines = -1  # not tracked in direct path
            n_individuals = -1
        else:
            raise ValueError(
                f'Unknown input_format \'{input_format}\'. '
                f'Use \'illumina\' or \'wide\'.'
            )

        elapsed = time.perf_counter() - start
        logger.info('Geno file conversion completed in %.2f seconds.', elapsed)
        logger.info(
            'GENO: %s  (%d SNPs, %d individuals)',
            output_path,
            n_lines,
            n_individuals,
        )

        return output_path

    # ==================================================================
    #  Internal helpers
    # ==================================================================

    def _read_data_headers(
        self, f: TextIOWrapper, nextline: int
    ) -> Tuple[Tuple[str, ...], int]:
        line = f.readline()
        nextline += 1
        if line == '':
            raise InvalidIlluminaReportError('No data header found in the report')
        headers = line.strip().split('\t')
        return tuple(headers), nextline

    def _read_report_headers(
        self, f: TextIOWrapper, nextline: int
    ) -> Tuple[dict[str, str], int]:
        headers: dict[str, str] = {}
        while True:
            line = f.readline()
            nextline += 1
            if line == '' or line.startswith('['):
                break
            vals = line.strip().split('\t')
            if len(vals) >= 2:
                headers[vals[0]] = vals[1]
        return headers, nextline

    @staticmethod
    def _write_lines(lines: list[str], path: Path, *, label: str = '') -> None:
        with open(path, 'w', encoding='utf-8', newline='\n') as fh:
            for line in lines:
                fh.write(line + '\n')
        if label:
            logger.info('Wrote %s file (%d lines): %s', label, len(lines), path)

    @staticmethod
    def _write_ids(individuals: list[str], path: Path) -> None:
        with open(path, 'w', encoding='utf-8', newline='\n') as fh:
            for ind in individuals:
                fh.write(ind + '\n')
        logger.info('Wrote ID file (%d individuals): %s', len(individuals), path)


