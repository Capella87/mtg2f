import subprocess
import logging
import time
from pathlib import Path

from exceptions import PipelineError

logger = logging.getLogger(__name__)


class PlinkCommandError(PipelineError):
    '''Exception raised when PLINK command fails'''
    def __init__(self, message: str, stderr: str = ''):
        self.message = message
        self.stderr = stderr
        super().__init__(self.message)

    def __str__(self):
        if self.stderr:
            return f'{self.message}\nStderr output:\n{self.stderr}'
        return self.message


class PlinkRunner:
    '''Class to run plink commands'''

    def __init__(self, name_prefix: str, plink_path: str, working_dir: str, output_name_prefix: str = None):
        self.name_prefix = name_prefix
        self.plink_path = Path(plink_path).absolute()
        self.working_dir = Path(working_dir).absolute()
        self.output_name_prefix = output_name_prefix if output_name_prefix else f'{name_prefix}_final'

        # Log file title with timestamp
        timestamp = time.strftime('%Y%m%d_%H%M%S_%z')
        self.log_file = self.working_dir / f'plink_run_{self.plink_path.stem}_{timestamp}.log'

    def run(self, *args) -> None:
        logger.info('STEP 3: Running PLINK QC')
        logger.info('PLINK QC Pipeline Starting at %s', time.strftime('%Y-%m-%d %H:%M:%S %z'))
        logger.debug('PLINK executable: %s', self.plink_path)
        logger.debug('Working directory: %s', self.working_dir)

        # Use string representation of plink path for subprocess
        plink_cmd = str(self.plink_path)

        if len(args) > 0:
            tasks = list(args)
            logging.debug('User provided PLINK tasks: %s. the default tasks are overridden.', tasks)
        else:
            tasks = [
                # PLINK QC
                [plink_cmd, '--file', self.name_prefix, '--make-bed', '--out', f'{self.name_prefix}_binary', '--cow', '--allow-no-sex'],
                [plink_cmd, '--bfile', f'{self.name_prefix}_binary', '--maf', '0.05', '--mind', '0.1',
                 '--geno', '0.1', '--hwe', '0.000001', '--make-bed', '--out', f'{self.output_name_prefix}',
                 '--cow', '--allow-no-sex'],
            ]

        for task in tasks:
            logger.info('Running PLINK command: %s', ' '.join(task))
            result = subprocess.run(
                task,
                capture_output=True,
                text=True,
                cwd=str(self.working_dir),
                check=False,
            )
            if result.returncode != 0:
                raise PlinkCommandError(f'PLINK command failed: {": ".join(task)}', stderr=result.stderr)
            else:
                logger.info('PLINK command completed successfully: %s', ' '.join(task))

