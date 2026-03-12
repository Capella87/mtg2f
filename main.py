import argparse
import logging
from pathlib import Path
import platform

from converter import IlluminaReportConverter
from check import check as check_dependencies
from runners.mtg2runner import Mtg2Runner
from runners.plinkrunner import PlinkRunner


def setup_logging(verbose: bool = False, log_file: str | None = None) -> None:
    """Configure root logger with console output."""
    level = logging.DEBUG if verbose else logging.INFO

    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    file_formatter = logging.Formatter(
        '%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S %z'
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    handlers: list[logging.Handler] = [console_handler]

    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(file_formatter)
        handlers.append(file_handler)

    root = logging.getLogger()
    root.setLevel(level)
    for handler in handlers:
        root.addHandler(handler)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog='mtg2f',
        description='mtg2f \u2014 Convert genotype files to PLINK format and run QC pipeline with MTG2.',
    )

    parser.add_argument(
        '--version',
        action='version',
        help='Show program version and exit.',
        version='mtg2f 0.1.0',
    )

    subparsers = parser.add_subparsers(dest='command', help=None)

    common_option_parser = argparse.ArgumentParser(add_help=False)
    common_option_parser.add_argument(
        '--verbose', '-V',
        action='store_true',
        help='Enable verbose (DEBUG) logging.',
    )
    common_option_parser.add_argument(
        '--log',
        action='store',
        help='Path to a log file to save log. In default, logs are only printed to console.',
    )

    convert_parser = subparsers.add_parser('convert',
                                           help='Convert genotype file to PLINK format',
                                           parents=[common_option_parser])
    convert_parser.set_defaults(func=convert)
    convert_parser.add_argument(
        'input',
        help='Path to the input genotype file.',
        default=None,
    )
    convert_parser.add_argument(
        'output',
        help='Output prefix (creates <prefix>.map, <prefix>.ped, <prefix>_id.txt).',
        default=None,
    )
    convert_parser.add_argument(
        '-f', '--format',
        choices=['illumina', 'wide', 'genotype'],
        default='illumina',
        help='Input format: \'illumina\' (Final Report, default), \'wide\' '
             '(SNP-major tab-delimited), or \'genotype\' (genotype file).',
    )
    convert_parser.add_argument(
        '--missing',
        default='-',
        help='Missing genotype string in the input file (default: \'-\' for '
             'Illumina, use \'NN\' for wide format).',
    )
    convert_parser.add_argument(
        '--min-count',
        type=int,
        default=0,
        help='Minimum genotype count; genotypes appearing <= this are set to '
             'missing (default: 0 = disabled).',
    )
    convert_parser.add_argument(
        '--sex',
        type=int,
        default=3,
        choices=[1, 2, 3],
        help='Default sex code: 1=male, 2=female, 3=unknown (default: 3).',
    )
    convert_parser.add_argument(
        '--phenotype',
        type=int,
        default=-9,
        help='Default phenotype value (default: -9 = missing).',
    )

    check_parser = subparsers.add_parser('check',
                                         help='Check and install mtg2 and plink dependencies',
                                         parents=[common_option_parser])
    check_parser.set_defaults(func=check)

    prepare_parser = subparsers.add_parser('prepare',
                                           help='Run PLINK QC pipeline for the converted files.',
                                           parents=[common_option_parser])
    prepare_parser.add_argument('input',
                                help='Input prefix of the converted PLINK files to run QC on (e.g. <prefix>).')
    prepare_parser.add_argument('output',
                                help='Output prefix of the converted PLINK files to run QC on (e.g. <prefix>_final). In default, the output files will be saved with the same prefix as the input files with "_final" suffix.',
                                default=None)

    prepare_parser.set_defaults(func=prepare)



    return parser.parse_args(argv)


def prepare(args: argparse.Namespace) -> None:
    setup_logging(args.verbose, log_file=args.log)
    dep_paths = check_dependencies(custom_path=None)

    plink_runner = PlinkRunner(name_prefix=args.input, plink_path=dep_paths['plink'], working_dir='.',
                               output_name_prefix=args.output)
    plink_runner.run()

    # mtg2_runner = Mtg2Runner(name_prefix=args.input, mtg2_path=dep_paths['mtg2'], working_dir='.', output_name_prefix=args.output)
    # mtg2_runner.create_grm_file()
    return


def convert(args: argparse.Namespace) -> None:
    setup_logging(args.verbose, log_file=args.log)
    converter = IlluminaReportConverter(
        missing_genotype=args.missing,
        min_genotype_count=args.min_count,
        sex=args.sex,
        phenotype=args.phenotype,
    )

    output_title = args.output
    if not output_title:
        output_title = f'{args.input}_output'

    if args.format != 'genotype':
        result = converter.convert_geno_file(
            args.input, output_title, input_format=args.format
        )
        logging.info('Conversion to geno txt file is completed. Saved as %s on %s', result.stem, str(result.absolute()))
    else:
        result = Path(args.input).absolute()
        logging.info('Input file is in genotype format. Skipping conversion to geno txt file. Using input file directly: %s', str(result.absolute()))

    # Conversion to plink files (from geno.txt)
    plink_format_conversion_result = converter.convert_file(
        result, output_title)

    logging.info('Conversion to plink files is completed. PLINK output files:')
    for key, path in plink_format_conversion_result.items():
        logging.info('%3s: %s', key, path)

def check(args: argparse.Namespace) -> None:
    setup_logging(args.verbose, log_file=args.log)
    _ = check_dependencies(custom_path=None)



def main(argv: list[str] | None = None) -> None:
    if platform.machine() not in ['x86_64', 'AMD64']:
        logging.error('Unsupported architecture: %s. mtg2f only supports x86_64/AMD64.', platform.machine())
        return

    args = parse_args(argv)
    if args.command is None:
        parse_args(['--help'])
        return
    args.func(args)

if __name__ == '__main__':
    main()
