        # ref_bin_prefix = f'{ref_prefix}_binary'
        # if not Path(f'{ref_bin_prefix}.bed').exists() or not Path(f'{ref_bin_prefix}.bim').exists() or not Path(f'{ref_bin_prefix}.fam').exists():
        #     logger.info('Converting reference herd data to PLINK binary formats:')
        #     try:
        #         task = [str(self.plink_path), '--file', ref_prefix, '--make-bed', '--out', ref_bin_prefix, '--cow', '--allow-no-sex']
        #         result = subprocess.run(
        #             task,
        #             capture_output = True,
        #             text = True,
        #             cwd = str(self.working_dir),
        #             check = False,
        #         )
        #         if result.returncode != 0:
        #             logger.error('PLINK command failed: %s', ' '.join([str(self.plink_path), '--file', ref_prefix, '--make-bed', '--out', ref_bin_prefix, '--cow', '--allow-no-sex']))
        #             logger.error('Error message: %s', result.stderr)
        #             raise PlinkCommandError(f'PLINK command failed: {' '.join(task)}', stderr=result.stderr)
        #         else:
        #             logger.info('PLINK command completed successfully: %s', ' '.join(task))
        #     except PlinkCommandError as e:
        #         logger.exception('An error occurred while converting reference herd data to PLINK binary formats.')
        #         logger.error('Command: %s', e.message)
        #         logger.error('Stderr: %s', e.stderr)
        #         raise RuntimeError from e

from pathlib import Path
import os
import logging
import subprocess

from exceptions import CommandError

logger = logging.getLogger(__name__)


def run_command(command: list[str], working_dir: Path, description: str = '',
                show_console_output: bool = False) -> None:
    """Run *command* in *working_dir*, raising CommandError on non-zero exit."""
    logger.info('Running command: %s', ' '.join(command))

    env = os.environ.copy()
    env['OMP_STACK_SIZE'] = '1G'

    result = subprocess.run(
        command,
        capture_output=not show_console_output,
        text=not show_console_output,
        env=env,
        cwd=str(working_dir),
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr if isinstance(result.stderr, str) else ''
        logger.error('Command failed: %s', ' '.join(command))
        if stderr:
            logger.error('Stderr: %s', stderr)
        raise CommandError(
            description or f'Command failed with exit code {result.returncode}',
            cmd=command,
            stderr=stderr,
        )
    logger.info('Command completed successfully: %s', ' '.join(command))


def run_command_with_bash_config(command: list[str], working_dir: Path,
                                 description: str = '') -> None:
    """Run *command* via bash (ulimit -s unlimited), raising CommandError on failure."""
    env = os.environ.copy()
    env['OMP_STACK_SIZE'] = '1G'
    command_str = ' '.join(command)
    bash_command = f'ulimit -s unlimited; {command_str}'

    logger.info('Running command: %s', bash_command)
    result = subprocess.run(
        ['bash', '-c', bash_command],
        env=env,
        cwd=str(working_dir),
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr if isinstance(result.stderr, str) else ''
        logger.error('Command failed: %s', bash_command)
        if stderr:
            logger.error('Stderr: %s', stderr)
        raise CommandError(
            description or f'Command failed with exit code {result.returncode}',
            cmd=command,
            stderr=stderr,
        )
    logger.info('Command completed successfully: %s', ' '.join(command))
