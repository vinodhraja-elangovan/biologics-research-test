import enum
from typing import List


class DnaSchemaType(str, enum.Enum):
    Sequence = 'Sequence'
    Feature = 'Feature'

    def get_computed_fields(self) -> List[str]:
        if self == DnaSchemaType.Sequence:
            return DnaSequenceFieldKey.get_computed_fields()
        elif self == DnaSchemaType.Feature:
            return DnaFeatureFieldKey.get_computed_fields()
        else:
            return []


class DnaSequenceFieldKey(str, enum.Enum):
    SourceSequencingRun = 'Source: Sequencing Run'
    SourceClone = 'Source: Clone'
    Type = 'Type'
    Species = 'Species'
    TranslationAA = 'Translation AA'
    TranslationAAResidues = 'Translation AA - Residues'
    Comment = 'Comment'

    @staticmethod
    def get_computed_fields() -> List['DnaSequenceFieldKey']:
        return [DnaSequenceFieldKey.Species, DnaSequenceFieldKey.TranslationAAResidues]


class DnaFeatureFieldKey(str, enum.Enum):
    Functions = 'Function(s)'
    SequenceSource = 'Sequence Source'
    SourceSpecies = 'Source Species'
    CodonOptimizationSpecies = 'Codon Optimization - Species'
    CodonOptimizationProvider = 'Codon Optimization - Provider'
    Comment = 'Comment'
    TranslationAA = 'Translation AA'
    TranslationAAResidues = 'Translation AA - Residues'

    @staticmethod
    def get_computed_fields() -> List['DnaFeatureFieldKey']:
        return [DnaFeatureFieldKey.TranslationAAResidues]


class AaSequenceFieldKey(str, enum.Enum):
    SequenceSource = 'Sequence Source'
    SourceClone = 'Source: Clone'
    Type = 'Type'
    IMGTVersion = 'IMGT Version'


class AaSequenceCustomFieldKey(str, enum.Enum):
    DNASequenceId = 'DNA Sequence ID'


class GermlineAlignmentCustomFieldKey(str, enum.Enum):
    DNASequenceId = 'DNA Sequence ID'
