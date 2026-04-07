from pathlib import Path
import logging

from exceptions import PipelineError
from utils import run_command

logger = logging.getLogger(__name__)


class GRMCreation:
    def __init__(self, gcta_path: Path, working_dir: Path = Path.cwd()):
        self.gcta_path = gcta_path if isinstance(gcta_path, Path) else Path(gcta_path)
        self.working_dir = Path(working_dir).absolute()

    def run(self, merged_prefix: str, grm_prefix: str = 'merged_all_grm') -> str:
        logger.info('STEP 7: Creating GRM from merged data')

        task = [
            str(self.gcta_path), '--bfile', merged_prefix,
            '--autosome', '--make-grm-gz', '--out', grm_prefix]
        run_command(task, self.working_dir, description='Creating GRM from merged data')

        grm_file = self.working_dir / f'{grm_prefix}.grm.gz'
        id_file = self.working_dir / f'{grm_prefix}.grm.id'

        if Path.exists(grm_file) and Path.exists(id_file):
            logger.info('GRM file created successfully: %s, %s', grm_file.name, id_file.name)
            return str(grm_file)
        else:
            raise PipelineError(f'GRM file creation failed: {grm_file} or {id_file} not found')
