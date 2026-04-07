from pathlib import Path
import logging

from exceptions import PipelineError

logger = logging.getLogger(__name__)


class GenoFileFilter:
    def __init__(self, geno_file: Path | str, prune_out_ids: set, prune_out_snps: set,
                 all_ids: list, all_snps: list):
        self.geno_file = Path(geno_file)
        self.prune_out_ids = prune_out_ids
        self.prune_out_snps = prune_out_snps
        self.all_ids = all_ids
        self.all_snps = all_snps

    def filter(self, output_file: Path | str = Path.cwd() / 'exp_geno_qc.txt') -> Path:
        logger.info('STEP 5: Generating QCed geno file:')
        remove_snp_idx = {i for i, snp in enumerate(self.all_snps) if snp in self.prune_out_snps}
        remove_id_idx = {i for i, iid in enumerate(self.all_ids) if iid in self.prune_out_ids}

        if not type(output_file) == Path:
            output_file = Path(output_file)
        output_file.unlink(missing_ok=True)
        try:
            logging.info('Filtering geno file to remove pruned items:')
            with open(self.geno_file, 'r', encoding='utf-8') as f_in, open(output_file, 'w', encoding='utf-8') as f_out:
                header = f_in.readline().strip().split('\t')
                new_header = header[:3] + [header[i] for i in range(3, len(header)) if (i - 3) not in remove_id_idx]
                f_out.write('\t'.join(new_header) + '\n')

                for idx, line in enumerate(f_in):
                    if idx in remove_snp_idx:
                        continue
                    parts = line.strip().split('\t')
                    if not parts:
                        continue
                    row_out = parts[:3] + [parts[i] for i in range(3, len(parts)) if (i - 3) not in remove_id_idx]
                    f_out.write('\t'.join(row_out) + '\n')
        except Exception as e:
            raise PipelineError(f'Error while filtering geno file: {e}') from e
        logger.info('✓ %s', output_file)
        return output_file
