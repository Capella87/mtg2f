"""Quality control module for genotype data."""

import logging

logger = logging.getLogger(__name__)


def check_genotype_count(
    genotypes: list[str],
    missing_genotype: str,
    snp_id: str,
    min_count: int = 0,
) -> list[str]:
    """Check genotype quality and replace low-count genotypes with missing.

    Counts occurrences of each non-missing genotype (e.g., AA, AB, BB).
    If any genotype appears fewer than `min_count` times, all instances of
    that genotype are replaced with the missing genotype marker.

    Args:
        genotypes: List of genotype strings for a SNP across all individuals.
        missing_genotype: The string representing a missing genotype (e.g., 'NN').
        snp_id: The SNP identifier (for logging purposes).
        min_count: Minimum occurrences required. Genotypes appearing fewer
                   times than this threshold are set to missing.

    Returns:
        The (possibly modified) list of genotypes.
    """
    if min_count <= 0:
        return genotypes

    # Count occurrences of each non-missing genotype
    geno_counts: dict[str, int] = {}
    for g in genotypes:
        if g != missing_genotype:
            geno_counts[g] = geno_counts.get(g, 0) + 1

    # Find genotypes below threshold
    low_genos = {g for g, count in geno_counts.items() if count <= min_count}
    if low_genos:
        logger.info(
            'SNP %s: genotypes %s appear <= %d times, setting to missing.',
            snp_id,
            low_genos,
            min_count,
        )
        genotypes = [
            missing_genotype if g in low_genos else g for g in genotypes
        ]

    return genotypes


def validate_biallelic(
    genotypes: list[str],
    missing_genotype: str,
    snp_id: str,
    line_number: int,
) -> None:
    """Validate that a SNP has at most 2 distinct alleles.

    Args:
        genotypes: List of genotype strings for a SNP.
        missing_genotype: The string representing missing data.
        snp_id: The SNP identifier.
        line_number: Line number in the input file (for error reporting).

    Raises:
        ValueError: If more than 2 distinct alleles are found.
    """
    alleles: set[str] = set()
    for g in genotypes:
        if g != missing_genotype:
            # Each genotype is 2 characters, e.g. 'AG' -> {'A', 'G'}
            for ch in g:
                alleles.add(ch)

    if len(alleles) > 2:
        raise ValueError(
            f'SNP \'{snp_id}\' (line {line_number}) has more than 2 alleles: '
            f'{sorted(alleles)}. Expected biallelic.'
        )
