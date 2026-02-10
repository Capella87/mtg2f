from io import TextIOWrapper
from abc import abstractmethod
from pathlib import Path
from typing import Any, Tuple, override

import logging
import os
import time

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

        df = pd.read_csv(
            filepath, sep='\t', skiprows=nextline, names=list(self.data_headers)
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

        with open(filepath, 'r', encoding='utf-8') as fh:
            header_line = fh.readline().rstrip('\n')
            header = header_line.split('\t')
            individuals = header[3:]

            if not individuals:
                raise ValueError(
                    'No individual columns found in the header. '
                    'Expected: SNP_id\\tchr\\tposition\\tInd1\\tInd2\\t…'
                )

            rows: list[dict] = []
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
                    rows.append(
                        {
                            snp_col: snp_id,
                            sample_col: ind_id,
                            a1_col: a1,
                            a2_col: a2,
                            chr_col: chromosome,
                            pos_col: position,
                        }
                    )

        df = pd.DataFrame(rows)
        logger.info(
            'Read wide-format file: %d rows (long), %d columns.',
            len(df),
            len(df.columns),
        )
        return df

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

        # --- Build combined genotype column ---
        data = data.copy()
        data['_geno'] = data[a1_col].astype(str) + data[a2_col].astype(str)

        # --- QC per SNP ---
        snp_to_index = {s: i for i, s in enumerate(snp_order)}
        ind_to_index = {s: i for i, s in enumerate(individuals)}
        missing_geno = self.missing_genotype + self.missing_genotype

        # Pre-allocate: geno_matrix[individual_idx][snp_idx]
        geno_matrix: list[list[str]] = [
            ['0\t0'] * n_snps for _ in range(n_ind)
        ]

        # Group by SNP for QC and filling
        for snp_name, grp in data.groupby(snp_col, sort=False):
            snp_idx = snp_to_index[snp_name]
            geno_list = grp['_geno'].tolist()
            sample_ids = grp[sample_col].tolist()

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
                    geno_matrix[ind_idx][snp_idx] = '0\t0'
                else:
                    if len(geno) != 2:
                        raise ValueError(
                            f'Unexpected genotype \'{geno}\' for SNP '
                            f'\'{snp_name}\', sample \'{sample_id}\'.'
                        )
                    geno_matrix[ind_idx][snp_idx] = f'{geno[0]}\t{geno[1]}'

        # --- Build PED lines ---
        sex = str(self.default_sex)
        phe = str(self.default_phenotype)
        ped_lines: list[str] = []
        for i, ind_id in enumerate(individuals):
            geno_str = '\t'.join(geno_matrix[i])
            ped_lines.append(
                f'{ind_id}\t{ind_id}\t0\t0\t{sex}\t{phe}\t{geno_str}'
            )

        return {
            'map': map_lines,
            'ped': ped_lines,
            'individuals': individuals,
        }

    # ==================================================================
    #  Convenience: read + convert + write in one call
    # ==================================================================

    def convert_file(
        self,
        input_file: str | os.PathLike,
        output_prefix: str | os.PathLike,
        input_format: str = 'illumina',
    ) -> dict[str, Path]:
        """Read a genotype file and write PLINK .map/.ped files.

        Args:
            input_file: Path to the input genotype file.
            output_prefix: Prefix for output files.  Creates
                ``<prefix>.map``, ``<prefix>.ped``, ``<prefix>_id.txt``.
            input_format: ``'illumina'`` for Illumina Final Report or
                ``'wide'`` for wide-format (SNP-major) files.

        Returns:
            Dictionary mapping ``'map'``, ``'ped'``, ``'id'`` to output
            :class:`~pathlib.Path` objects.
        """
        start = time.perf_counter()
        input_path = Path(input_file)
        output_prefix = Path(output_prefix)

        # Read
        if input_format == 'illumina':
            df = self.read_illumina_report(input_path)
        # elif input_format == 'wide':
        #     df = self.read_wide_format(input_path)
        else:
            raise ValueError(
                f'Unknown input_format \'{input_format}\'. '
                f'Use \'illumina\' or \'wide\'.'
            )

        # Convert
        result = self.convert(df)

        # Write
        map_path = output_prefix.with_suffix('.map')
        ped_path = output_prefix.with_suffix('.ped')
        id_path = Path(f'{output_prefix}_id.txt')

        self._write_lines(result['map'], map_path, label='MAP')
        self._write_lines(result['ped'], ped_path, label='PED')
        self._write_ids(result['individuals'], id_path)

        elapsed = time.perf_counter() - start
        logger.info('Conversion completed in %.2f seconds.', elapsed)
        logger.info('  MAP : %s  (%d SNPs)', map_path, len(result['map']))
        logger.info('  PED : %s  (%d individuals)', ped_path, len(result['ped']))
        logger.info('  IDs : %s', id_path)

        return {'map': map_path, 'ped': ped_path, 'id': id_path}

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
        with open(path, 'w', encoding='utf-8') as fh:
            for line in lines:
                fh.write(line + '\n')
        if label:
            logger.info('Wrote %s file (%d lines): %s', label, len(lines), path)

    @staticmethod
    def _write_ids(individuals: list[str], path: Path) -> None:
        with open(path, 'w', encoding='utf-8') as fh:
            for ind in individuals:
                fh.write(ind + '\n')
        logger.info('Wrote ID file (%d individuals): %s', len(individuals), path)
