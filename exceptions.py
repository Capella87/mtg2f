"""
Pipeline exception hierarchy for mtg2f.

All exceptions that should halt pipeline execution are subclasses of
PipelineError.  Catch PipelineError at the top level (main()) to print a
clean error message and exit without a raw Python traceback.
"""


class PipelineError(Exception):
    """Base class for all pipeline-halting errors."""


class CommandError(PipelineError):
    """Raised when an external command (PLINK, GCTA, MTG2) returns non-zero."""

    def __init__(self, message: str, cmd: list[str] | None = None, stderr: str = ''):
        self.cmd = cmd or []
        self.stderr = stderr
        super().__init__(message)

    def __str__(self) -> str:
        parts = [super().__str__()]
        if self.cmd:
            parts.append(f'Command: {" ".join(self.cmd)}')
        if self.stderr:
            parts.append(f'Stderr:\n{self.stderr}')
        return '\n'.join(parts)


class MissingInputError(PipelineError):
    """Raised when a required input file or directory is not found."""


class PipelineStepError(PipelineError):
    """Raised by a pipeline step when it cannot complete successfully."""

    def __init__(self, step: str, message: str):
        self.step = step
        super().__init__(f'[{step}] {message}')
