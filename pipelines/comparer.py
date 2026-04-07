from pathlib import Path
import logging

from exceptions import PipelineError

logger = logging.getLogger(__name__)

class PrunedComparer:
    def __init__(self, exp_prefix: str, qc_prefix: str, id_path: Path | str):
        self.exp_prefix = exp_prefix
        self.qc_prefix = qc_prefix
        self.id_path = id_path

    def run(self):
        logger.info('STEP 4: Comparing pruned items between the original data and QC results:')

        prune_in_ids = set()
        try:
            with open(f'{self.qc_prefix}.fam', 'r', encoding='utf-8') as f:
                for line in f:
                    parts = line.split()
                    if len(parts) >= 2:
                        prune_in_ids.add(parts[1])

            prune_in_snps = set()
            with open(f'{self.qc_prefix}.bim', 'r', encoding='utf-8') as f:
                for line in f:
                    parts = line.split('\t')
                    if len(parts) >= 2:
                        prune_in_snps.add(parts[1])

            all_ids = []
            prune_out_ids = set()
            with open(self.id_path, 'r', encoding='utf-8') as f:
                for line in f:
                    iid = line.strip()
                    all_ids.append(iid)
                    if iid not in prune_in_ids:
                        prune_out_ids.add(iid)

            all_snps = []
            prune_out_snps = set()
            with open(f'{self.exp_prefix}.map', 'r', encoding='utf-8') as f:
                for line in f:
                    parts = line.strip().split('\t')
                    if len(parts) >= 2:
                        snp = parts[1]
                        all_snps.append(snp)
                        if snp not in prune_in_snps:
                            prune_out_snps.add(snp)
        except Exception as e:
            raise PipelineError(f'Error while comparing pruned items: {e}') from e

        logger.info('Removed ID: %d, Removed SNP: %d', len(prune_out_ids), len(prune_out_snps))
        logger.info('Remaining ID: %d, Remaining SNP: %d', len(prune_in_ids), len(prune_in_snps))
        return prune_out_ids, prune_out_snps, all_ids, all_snps
