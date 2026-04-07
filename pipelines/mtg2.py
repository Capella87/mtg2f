from pathlib import Path

import logging
import pandas as pd

from exceptions import PipelineError
from utils import run_command_with_bash_config

logger = logging.getLogger(__name__)


class Mtg2Analysis:
    def __init__(self, mtg2_path: Path, working_dir: Path = Path.cwd(),
                 num_traits: int = 4):
        self.mtg2_path = mtg2_path if isinstance(mtg2_path, Path) else Path(mtg2_path)
        self.working_dir = Path(working_dir).absolute()
        self.num_traits = num_traits

    def _prepare_reml_cove_file(self, cove_filename: str) -> Path:
        cove_file_path = self.working_dir / cove_filename
        if not Path.exists(cove_file_path):
            raise PipelineError(f'Covariate file not found: {cove_file_path}')

        try:
            with open(cove_file_path, 'w', encoding='utf-8') as f:
                for _ in range(4):
                    f.write('ve 0.7\n')
                for _ in range(6):
                    f.write('cov 0.01\n')
                for _ in range(4):
                    f.write('va 0.3\n')
                for _ in range(6):
                    f.write('cov 0.01\n')
            return cove_file_path
        except Exception as e:
            raise PipelineError(f'Error while preparing REML covariate file: {e}') from e

    def _config_cov_options(self, cc_cov_file_path: Path, qc_cov_file_path: Path) -> list:
        cov_options = []
        cov_cc_matched_file = self.working_dir / f'{cc_cov_file_path.name}.matched'
        cov_qc_matched_file = self.working_dir / f'{qc_cov_file_path.name}.matched'

        if Path.exists(cov_cc_matched_file):
            cov_options += ['-cc', cov_cc_matched_file.name]
        if Path.exists(cov_qc_matched_file):
            cov_options += ['-qc', cov_qc_matched_file.name]
        return cov_options

    def run_reml(self, grm_prefix: str,
                cc_cov_file_path: Path | str,
                qc_cov_file_path: Path | str,
                sv_multivariate_cov_filename: str = 'sv_multivariate_cove.txt',
                multivariate_result_path: Path | str = Path.cwd() / 'multivariate_result') -> str:
        logger.info('STEP 9: Running MTG2 REML analysis')

        ## TODO: You need to detect whether filename or prefix contains path (e.g. ./ or ../)
        sv_multivariate_cov_file_path = self._prepare_reml_cove_file(sv_multivariate_cov_filename)
        cov_opts = self._config_cov_options(Path(cc_cov_file_path), Path(qc_cov_file_path))

        command = [
            'mtg2', '-p', 'total_animals_fam.id',
            '-d', 'multivariate.pheno',
            '-zg', f'{grm_prefix}.grm.gz',
            '-out', str(multivariate_result_path),
            '-mod', str(self.num_traits),
            '-cove', '1',
        ] + cov_opts + [
            '-sv', str(sv_multivariate_cov_file_path)
        ]

        run_command_with_bash_config(command, self.working_dir, 'Run REML analysis with MTG2')

        if Path.exists(multivariate_result_path):
            logger.info('REML analysis completed successfully: %s', multivariate_result_path)
        else:
            raise PipelineError(f'REML analysis failed: Result file not found at {multivariate_result_path}')
        return str(multivariate_result_path)

    def run_blup_and_gebv(self, grm_prefix: str,
                          cc_cov_file_path: Path | str,
                          qc_cov_file_path: Path | str,
                          blup_result_path: Path | str = Path.cwd() / 'multivariate_blup',
                          multivariate_result_path: Path | str = Path.cwd() / 'multivariate_result',
                          ebv_output_prefix: str = 'multivariate_ebv',
                          trait_names: dict = None,
                          prediction_result_prefix: str = 'prediction_results.csv') -> dict:
        """
        Run MTG2 BLUP and extract GEBV values, generating prediction results.

        Args:
            grm_prefix: Prefix of GRM files
            cc_cov_file_path: Path to categorical covariate file
            qc_cov_file_path: Path to quantitative covariate file
            total_animals_fam_id_path: Path to total animals fam ID file
            multivariate_pheno_path: Path to phenotype file
            sv_multivariate_cov_filename: Starting values for covariance file
            blup_result_path: Output path for BLUP results
            trait_names: Dictionary mapping trait numbers to names (e.g., {1: 'Trait1', ...})

        Returns:
            Dictionary with keys: prediction_results_df, prediction_results_all_df,
                                   prediction_results_path, all_results_path, ebv_data
        """
        logger.info('STEP 10: Running MTG2 BLUP and GEBV extraction')

        # Ensure default trait names if not provided
        if trait_names is None:
            trait_names = {1: '도체중', 2: '등심단면적', 3: '등지방두께', 4: '근내지방도'}

        cov_opts = self._config_cov_options(Path(cc_cov_file_path), Path(qc_cov_file_path))

        command = [
            'mtg2', '-p', 'total_animals_fam.id',
            '-d', 'multivariate.pheno',
            '-zg', f"{grm_prefix}.grm.gz",
            '-out', str(blup_result_path),
            '-mod', str(self.num_traits),
            '-cove', '1',
        ] + cov_opts + [
            '-sv', str(multivariate_result_path),
            '-bv', str(ebv_output_prefix),
            '-nit', '0'
        ]

        logger.info('Running command: %s', ' '.join(command))
        run_command_with_bash_config(command, self.working_dir, 'Run MTG2 BLUP and GEBV extraction')

        # Validate EBV file exists
        ebv_file_path = self.working_dir / str(ebv_output_prefix)
        if not ebv_file_path.exists():
            raise PipelineError(f'EBV file not created ({ebv_output_prefix} not found)')

        # Validate EBV file has data
        with open(ebv_file_path, 'r', encoding='utf-8') as f:
            ebv_lines = f.readlines()
        ebv_data_lines = [l for l in ebv_lines if l.strip() and
                         'trait' not in l.lower() and 'ebv' not in l.lower()]
        if len(ebv_data_lines) == 0:
            raise PipelineError('EBV file is empty (possible MTG2 segfault). '
                              'Check ulimit -s unlimited and available memory')

        logger.info('EBV file validated: %d data lines', len(ebv_data_lines))

        # Parse GRM ID file
        grm_id_path = self.working_dir / f'{grm_prefix}.grm.id'
        grm_id = pd.read_csv(grm_id_path, sep=r'\s+', header=None, dtype=str)
        grm_id.columns = ['FID', 'IID']
        total_individuals = len(grm_id)
        logger.info('Total individuals in GRM: %d', total_individuals)

        # Find individuals with all NA phenotypes
        pheno_path = self.working_dir / 'multivariate.pheno'
        na_rows = []
        with open(pheno_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f, 1):
                parts = line.strip().split()
                if len(parts) > 2 and all(v == 'NA' for v in parts[2:]):
                    na_rows.append(i)
        logger.info('Individuals with all NA phenotypes: %d', len(na_rows))

        # Parse EBV data by trait
        ebv_data = {t: [] for t in range(1, self.num_traits + 1)}
        with open(ebv_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or 'trait' in line.lower() or 'ebv' in line.lower():
                    continue
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        trait_num = int(parts[0])
                        ebv_value = float(parts[1])
                        if trait_num in ebv_data:
                            ebv_data[trait_num].append(ebv_value)
                    except ValueError:
                        continue

        # Validate EBV counts
        for t in range(1, self.num_traits + 1):
            actual = len(ebv_data[t])
            if actual != total_individuals:
                logger.warning('Trait %d EBV count: %d (expected: %d)', t, actual, total_individuals)

        # Generate prediction results for NA individuals
        results = []
        for row_num in na_rows:
            idx = row_num - 1
            row = {
                'FID': grm_id.iloc[idx]['FID'] if idx < total_individuals else 'Unknown',
                'IID': grm_id.iloc[idx]['IID'] if idx < total_individuals else 'Unknown',
            }
            for t in range(1, self.num_traits + 1):
                col_name = f'{trait_names[t]}_GEBV'
                if t in ebv_data and idx < len(ebv_data[t]):
                    row[col_name] = ebv_data[t][idx]
                else:
                    row[col_name] = None
            results.append(row)

        df_pred = pd.DataFrame(results)
        pred_path = self.working_dir / prediction_result_prefix
        df_pred.to_csv(pred_path, index=False, float_format='%.6f', encoding='utf-8-sig')
        logger.info('Prediction results saved: %s (%d individuals)', pred_path, len(df_pred))

        # Generate overall results for all individuals
        pheno_lines = []
        with open(pheno_path, 'r', encoding='utf-8') as f:
            pheno_lines = f.readlines()

        all_rows = []
        for i in range(total_individuals):
            row = {'FID': grm_id.iloc[i]['FID'], 'IID': grm_id.iloc[i]['IID']}

            if i < len(pheno_lines):
                parts = pheno_lines[i].strip().split()
                row['phenotype_status'] = 'all_NA' if (len(parts) > 2 and
                                                       all(v == 'NA' for v in parts[2:])) else 'observed'
            else:
                row['phenotype_status'] = 'unknown'

            for t in range(1, self.num_traits + 1):
                col_name = f'{trait_names[t]}_GEBV'
                if t in ebv_data and i < len(ebv_data[t]):
                    row[col_name] = ebv_data[t][i]
                else:
                    row[col_name] = None
            all_rows.append(row)

        df_all = pd.DataFrame(all_rows)
        all_path = self.working_dir / f'{prediction_result_prefix.replace(".csv", "_all.csv")}'
        df_all.to_csv(all_path, index=False, float_format='%.6f', encoding='utf-8-sig')
        logger.info('All results saved: %s (%d individuals)', all_path, len(df_all))

        # Log prediction results summary
        logger.info('\n═══ GEBV Prediction Results ═══')
        logger.info('\n%s', df_pred.to_string(index=False, float_format='%.4f'))

        return {
            'prediction_results_df': df_pred,
            'prediction_results_all_df': df_all,
            'prediction_results_path': str(pred_path),
            'all_results_path': str(all_path),
            'ebv_data': ebv_data,
            'n_predicted': len(df_pred),
            'n_total': total_individuals
        }
