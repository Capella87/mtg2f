import argparse
import logging

from converter import IlluminaReportConverter


def setup_logging(verbose: bool = False) -> None:
    """Configure root logger with console output."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(levelname)s: %(message)s',
        handlers=[logging.StreamHandler()],
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog='mtg2f',
        description='mtg2f \u2014 Convert genotype files to PLINK format',
    )

    parser.add_argument(
        '--version',
        action='version',
        help='Show program version and exit.',
        version='mtg2f 1.0.0',
    )

    subparsers = parser.add_subparsers(dest='command', help=None, required=True)

    common_option_parser = argparse.ArgumentParser(add_help=False)
    common_option_parser.add_argument(
        '--verbose', '-V',
        action='store_true',
        help='Enable verbose (VERBOSE) logging.',
    )

    convert_parser = subparsers.add_parser('convert',
                                           help='Convert genotype file to PLINK format',
                                           parents=[common_option_parser])
    convert_parser.set_defaults(func=convert)
    convert_parser.add_argument(
        'input',
        help='Path to the input genotype file.',
    )
    convert_parser.add_argument(
        'output',
        help='Output prefix (creates <prefix>.map, <prefix>.ped, <prefix>_id.txt).',
    )
    convert_parser.add_argument(
        '-f', '--format',
        choices=['illumina', 'wide'],
        default='illumina',
        help='Input format: \'illumina\' (Final Report, default) or \'wide\' '
             '(SNP-major tab-delimited).',
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

    return parser.parse_args(argv)


def convert(args: argparse.Namespace) -> None:
    setup_logging(args.verbose)
    converter = IlluminaReportConverter(
        missing_genotype=args.missing,
        min_genotype_count=args.min_count,
        sex=args.sex,
        phenotype=args.phenotype,
    )
    result = converter.convert_file(
        args.input, args.output, input_format=args.format
    )

    print('\nDone! Output files:')
    for key, path in result.items():
        print(f'  {key:>3}: {path}')



def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    args.func(args)

if __name__ == '__main__':
    main()
