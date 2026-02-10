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
        'input',
        help='Path to the input genotype file.',
    )
    parser.add_argument(
        'output',
        help='Output prefix (creates <prefix>.map, <prefix>.ped, <prefix>_id.txt).',
    )
    parser.add_argument(
        '-f', '--format',
        choices=['illumina', 'wide'],
        default='illumina',
        help='Input format: \'illumina\' (Final Report, default) or \'wide\' '
             '(SNP-major tab-delimited).',
    )
    parser.add_argument(
        '--missing',
        default='-',
        help='Missing genotype string in the input file (default: \'-\' for '
             'Illumina, use \'NN\' for wide format).',
    )
    parser.add_argument(
        '--min-count',
        type=int,
        default=0,
        help='Minimum genotype count; genotypes appearing <= this are set to '
             'missing (default: 0 = disabled).',
    )
    parser.add_argument(
        '--sex',
        type=int,
        default=3,
        choices=[1, 2, 3],
        help='Default sex code: 1=male, 2=female, 3=unknown (default: 3).',
    )
    parser.add_argument(
        '--phenotype',
        type=int,
        default=-9,
        help='Default phenotype value (default: -9 = missing).',
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose (DEBUG) logging.',
    )

    parser.add_argument(
        '--version',
        action='version',
        
    )

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
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


if __name__ == '__main__':
    main()
