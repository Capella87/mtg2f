import subprocess
import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)


class Mtg2CommandError(Exception):
    '''Exception raised when MTG2 command fails'''
    def __init__(self, message: str, stderr: str = ''):
        self.message = message
        self.stderr = stderr
        super().__init__(self.message)

    def __str__(self):
        if self.stderr:
            return f'{self.message}\nStderr output:\n{self.stderr}'
        return self.message


class Mtg2Runner:
    '''Class to run MTG2 commands'''

    def __init__(self, name_prefix: str, mtg2_path: str, working_dir: str, output_name_prefix: str = None):
        self.name_prefix = name_prefix
        self.mtg2_path = Path(mtg2_path).absolute()
        self.working_dir = Path(working_dir).absolute()
        self.output_name_prefix = output_name_prefix if output_name_prefix else self.name_prefix

        # Log file title with timestamp
        timestamp = time.strftime('%Y%m%d_%H%M%S_%z')
        self.log_file = self.working_dir / f'mtg2_run_{self.mtg2_path.stem}_{timestamp}.log'

    def create_grm_file(self, grm_name_prefix: str | None = None) -> Path:
        """ Create a grm file using MTG2. The grm file will be saved in the working directory with the name format: {grm_name_prefix}.grm or {output_name_prefix}.grm if grm_name_prefix is not provided. """
        mtg2_executable = str(self.mtg2_path)
        grm_file_path = self.working_dir / f'{self.output_name_prefix}.grm' if not grm_name_prefix else self.working_dir / f'{grm_name_prefix}.grm'
        tasks = [
            [mtg2_executable, '-plink', self.name_prefix, '-frq', '1'],
            [mtg2_executable, '-plink', self.name_prefix, '-rtmx', '-1', '-out', grm_file_path.stem]
        ]

        try:
            logger.info('MTG2 GRM Creation Starting at %s', time.strftime('%Y-%m-%d %H:%M:%S %z'))
            for task in tasks:
                logger.info('Running MTG2 command: %s', ' '.join(task))
                result = subprocess.run(
                    task,
                    capture_output=True,
                    text=True,
                    cwd=str(self.working_dir),
                    check=False,
                )
                if result.returncode != 0:
                    logger.error('MTG2 command failed: %s', ' '.join(task))
                    logger.error('Error message: %s', result.stderr)
                    raise Mtg2CommandError(f'MTG2 command failed: {" ".join(task)}', stderr=result.stderr)
                else:
                    logger.info('MTG2 command completed successfully: %s', ' '.join(task))
            logger.info('MTG2 successfully created GRM file: %s', grm_file_path)
        except Mtg2CommandError as e:
            logger.exception('An error occurred while running MTG2 commands.')
            logger.error('Command: %s', e.message)
            logger.error('Stderr: %s', e.stderr)
            raise RuntimeError from e

    # def run(self, *args) -> None:
    #     try:
