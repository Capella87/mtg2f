from pathlib import Path
import logging

from utils import run_command
from exceptions import CommandError, PipelineError

logger = logging.getLogger(__name__)


class MergePhenotypes:
    def __init__(self, working_dir: Path = Path.cwd()) -> None:
        self.working_dir = Path(working_dir).absolute()

    def run(self, grm_prefix: str,
            phenotype_file: str,
            cc_cov_file_path: Path | str,
            qc_cov_file_path: Path | str) -> str:
        """
        Merge phenotype and covariate data for all animals in GRM.

        Args:
            grm_prefix: Prefix for GRM files (e.g., 'merged_all_grm')
            phenotype_file: Path to phenotype file (e.g., 'PYNO.fam')
            cc_cov_file_path: Path to classification covariate file
            qc_cov_file_path: Path to quantitative covariate file

        Returns:
            grm_prefix (unchanged)
        """
        logger.info('STEP 8: Merging phenotype and covariate data')

        import pandas as pd

        def normalize_id(x):
            return str(x).strip()

        # Load GRM ID (reference)
        grm_id_path = f'{grm_prefix}.grm.id'
        try:
            grm_id = pd.read_csv(grm_id_path, sep=r'\s+', header=None, dtype=str)
            grm_id.columns = ['FID', 'IID']
            grm_id['norm'] = grm_id['IID'].apply(normalize_id)
            logger.info('GRM animals: %d', len(grm_id))
        except Exception as e:
            raise PipelineError(f'Failed to read GRM ID file {grm_id_path}: {e}') from e

        # Load reference phenotypes
        try:
            pheno_df = pd.read_csv(phenotype_file, sep=r'\s+', header=None, dtype=str)
            pheno_df['norm'] = pheno_df[1].apply(normalize_id)
            pheno_dict = {row['norm']: row for _, row in pheno_df.iterrows()}
            logger.info('Phenotype records: %d', len(pheno_df))
        except Exception as e:
            raise PipelineError(f'Failed to read phenotype file {phenotype_file}: {e}') from e

        # Create total_animals_fam.id (6-column FAM format)
        fam_out = self.working_dir / 'total_animals_fam.id'
        try:
            with open(fam_out, 'w', encoding='utf-8') as f:
                for _, row in grm_id.iterrows():
                    f.write(f"{row['FID']} {row['IID']} 0 0 0 -9\n")
        except Exception as e:
            raise PipelineError(f'Failed to write {fam_out}: {e}') from e

        # Create multivariate phenotype file (FID IID T1 T2 T3 T4)
        pheno_out = self.working_dir / 'multivariate.pheno'
        trait_cols = [2, 3, 4, 5]
        matched_count = 0
        na_count = 0

        try:
            with open(pheno_out, 'w', encoding='utf-8') as f:
                for _, grow in grm_id.iterrows():
                    fid, iid, norm = grow['FID'], grow['IID'], grow['norm']
                    vals = []

                    if norm in pheno_dict:
                        prow = pheno_dict[norm]
                        has_value = False
                        for ci in trait_cols:
                            raw = str(prow[ci]).strip() if ci < len(prow) else 'NA'
                            try:
                                fval = float(raw)
                                if pd.isna(fval):
                                    vals.append('NA')
                                else:
                                    vals.append(raw)
                                    has_value = True
                            except (ValueError, TypeError):
                                vals.append('NA')
                        if has_value:
                            matched_count += 1
                        else:
                            na_count += 1
                    else:
                        vals = ['NA'] * 4
                        na_count += 1

                    f.write(f"{fid} {iid} {' '.join(vals)}\n")
        except Exception as e:
            raise PipelineError(f'Failed to write {pheno_out}: {e}') from e

        logger.info('Phenotype matching: observed=%d, NA=%d', matched_count, na_count)

        # Validation: require at least one matched phenotype
        if matched_count == 0:
            raise PipelineError(
                f'No phenotypes matched between GRM and phenotype file.\n'
                f'Check if IDs in {phenotype_file} match GRM IDs.\n'
                f'Example GRM ID: {grm_id.iloc[0]["IID"]}\n'
                f'Example phenotype ID: {pheno_df.iloc[0][1]}'
            )

        # Match covariates
        for cov_path, cov_name in [(cc_cov_file_path, 'classification'),
                                    (qc_cov_file_path, 'quantitative')]:
            cov_path = Path(cov_path)
            if not cov_path.exists():
                logger.info('Skipping %s covariate file (not found): %s', cov_name, cov_path)
                continue

            try:
                cov_df = pd.read_csv(str(cov_path), sep=r'\s+', header=None, dtype=str)
                cov_df['norm'] = cov_df[1].apply(normalize_id)
                cov_dict = {row['norm']: row for _, row in cov_df.iterrows()}
                cov_col_start = 2
                cov_col_end = len(cov_df.columns) - 1
            except Exception as e:
                logger.warning('Failed to read %s covariate file: %s', cov_name, e)
                continue

            out_path = cov_path.parent / f'{cov_path.name}.matched'
            try:
                with open(out_path, 'w', encoding='utf-8') as f:
                    for _, grow in grm_id.iterrows():
                        fid, iid, norm = grow['FID'], grow['IID'], grow['norm']
                        if norm in cov_dict:
                            crow = cov_dict[norm]
                            cov_vals = [str(crow[c]) for c in range(cov_col_start, cov_col_end)]
                            f.write(f"{fid} {iid} {' '.join(cov_vals)}\n")
                        else:
                            cov_vals = ['NA'] * (cov_col_end - cov_col_start)
                            f.write(f"{fid} {iid} {' '.join(cov_vals)}\n")
                logger.info('✓ %s.matched (%s)', cov_path.name, cov_name)
            except Exception as e:
                logger.warning('Failed to write %s covariate matches: %s', cov_name, e)
                continue

        logger.info('✓ total_animals_fam.id, multivariate.pheno')
        return grm_prefix



class MergeHerdData:
    def __init__(self, plink_path: Path, working_dir: Path = Path.cwd()):
        self.plink_path = plink_path if isinstance(plink_path, Path) else Path(plink_path)
        self.working_dir = Path(working_dir).absolute()

    def _convert_ref_to_binary(self, ref_prefix: str, ref_bin_prefix: str) -> None:
        task = [str(self.plink_path), '--file', ref_prefix, '--make-bed',
                '--out', ref_bin_prefix, '--cow', '--allow-no-sex']
        # CommandError propagates automatically if the command fails
        run_command(task, self.working_dir,
                    description='Converting reference herd data to PLINK binary formats')

    def _merge_qc_and_ref(self, qc_prefix: str, ref_bin_prefix: str,
                          merged_result_prefix: str = 'merged_all') -> None:
        logger.info('Merging QCed data with reference herd data:')
        merge_task = [
            str(self.plink_path), '--bfile', qc_prefix,
            '--bmerge', f'{ref_bin_prefix}.bed', f'{ref_bin_prefix}.bim', f'{ref_bin_prefix}.fam',
            '--make-bed', '--out', merged_result_prefix, '--cow', '--allow-no-sex',
        ]
        try:
            run_command(merge_task, self.working_dir,
                        description='Merging QCed data with reference herd data')
        except CommandError:
            missnp = self.working_dir / f'{merged_result_prefix}-merge.missnp'
            if not missnp.exists():
                raise
            logger.warning('PLINK merge failed due to strand issues. Attempting to flip SNPs and merge again.')
            flipped_prefix = f'{qc_prefix}_flipped'
            flip_task = [
                str(self.plink_path), '--bfile', qc_prefix,
                '--flip', str(missnp),
                '--make-bed', '--out', flipped_prefix, '--cow', '--allow-no-sex',
            ]
            run_command(flip_task, self.working_dir, description='Flipping SNPs in QCed data')
            retry_task = [
                str(self.plink_path), '--bfile', flipped_prefix,
                '--bmerge', f'{ref_bin_prefix}.bed', f'{ref_bin_prefix}.bim', f'{ref_bin_prefix}.fam',
                '--autosome', '--make-bed', '--out', merged_result_prefix, '--cow', '--allow-no-sex',
            ]
            run_command(retry_task, self.working_dir,
                        description='Merging QCed (flipped) data with reference herd data')

    def _count_merged_results(self, merged_prefix: str) -> tuple[int, int]:
        try:
            with open(f'{merged_prefix}.fam', 'r', encoding='utf-8') as f:
                n_merged = sum(1 for _ in f)
            with open(f'{merged_prefix}.bim', 'r', encoding='utf-8') as f:
                n_snps = sum(1 for _ in f)
            logger.info('✓ %s.bed/bim/fam (Entity Count: %d, %d SNPs)', merged_prefix, n_merged, n_snps)
            return n_merged, n_snps
        except Exception as e:
            raise PipelineError(f'Failed to read merged PLINK files for validation: {e}') from e

    def run(self, qc_prefix: str, ref_prefix: str,
            merged_prefix: str = 'merged_all') -> str:
        logger.info('STEP 6: Merging reference herd data with QCed (Experimental) data:')

        # PLINK conversion of reference herd data
        ref_bin_prefix = f'{ref_prefix}_binary'
        if not (Path(f'{ref_bin_prefix}.bed').exists()
                and Path(f'{ref_bin_prefix}.bim').exists()
                and Path(f'{ref_bin_prefix}.fam').exists()):
            self._convert_ref_to_binary(ref_prefix, ref_bin_prefix)
        self._merge_qc_and_ref(qc_prefix, ref_bin_prefix, merged_prefix)

        # Validation
        _ = self._count_merged_results(merged_prefix)

        return merged_prefix
