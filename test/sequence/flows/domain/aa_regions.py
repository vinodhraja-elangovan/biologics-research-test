import logging
import re
from dataclasses import dataclass
from typing import Dict

logger = logging.getLogger(__name__)


@dataclass
class AaRegion:
    """
    Represents an amino acid region.

    Attributes:
        start (int): 0-based inclusive start index.
        end (int): 0-based exclusive end index. The end of the sequence is always represented as 0.

    Methods:
        create(start: int, end: int, sequence: str) -> AaRegion: Creates a new AaRegion instance.

    Usage:
        region = AaRegion.create(1, 10, 'ABCDEFGHIJ')
    """

    start: int
    end: int

    @staticmethod
    def create(start: int, end: int, sequence: str):
        """
        Creates an AaRegion object based on the given start, end, and sequence parameters.

        :param start: The starting position of the region.
        :param end: The ending position of the region.
        :param sequence: The amino acid sequence.
        :return: An AaRegion object representing the specified region.

        .. note::
            If the length of the sequence is equal to the end position, the end position will be set to 0.

        """

        # force end to 0 if equals to amino acids length
        if len(sequence) == end:
            end = 0

        return AaRegion(start, end)


class AaRegions:
    """
    AaRegions class is used to detect amino acid regions in a given amino acid sequence based on provided patterns.
    """

    @staticmethod
    def detect(amino_acids: str, **kwargs) -> Dict[str, AaRegion]:
        """
        Detects amino acid regions in a given amino acid sequence based on provided patterns.

        :param amino_acids: A string representing the amino acid sequence to be analyzed.
        :param kwargs: Keyword arguments representing different amino acid regions to be detected.
            The keys represent the names of the regions, and the values represent the patterns to match.
            Only string values are considered, any other value will be ignored.

        :return: A dictionary containing the detected amino acid regions.
            The keys of the dictionary represent the names of the regions,
            and the values are instances of the `AaRegion` class, which represents a detected region.
            If no regions are detected, an exception is raised.
        """

        # Filter string values only (excluding NaN, etc)
        non_empty_regions = {key: value for key, value in kwargs.items() if isinstance(value, str)}

        # Create pattern with filtered values, i.e .*FWR2_VALUE.*CDR2_VALUE.*FWR3_VALUE.*CDR3_VALUE.*FWR4_VALUE.*
        pattern_groups = [f'({re.escape(value)})' for key, value in non_empty_regions.items()]
        pattern = '.*'.join(pattern_groups)
        pattern = rf'.*{pattern}.*'

        result = re.search(pattern, amino_acids)

        if not result:
            logger.error('Unable to parse amino_acids using RegExp', extra={'amino_acids': amino_acids, **kwargs})

            raise Exception('Unable to parse amino_acids using RegExp')

        # Create dictionary with provided values {'fwr1': Region(start=..., end=...), ...}
        keys = list(non_empty_regions.keys())

        return {
            keys[i - 1]: AaRegion.create(result.start(i), result.end(i), amino_acids)
            for i in range(1, len(keys) + 1)
        }
