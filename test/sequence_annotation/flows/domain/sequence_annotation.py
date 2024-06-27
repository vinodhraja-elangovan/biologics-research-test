from dataclasses import dataclass
from typing import Dict, Any, Optional, List

from benchling_sdk.models import DnaSequence, EntityRegisteredEvent, AaSequence, AaSequenceCreate, \
    AaSequenceUpdate
from dataclasses_json import dataclass_json, DataClassJsonMixin

from sequence_annotation.flows.base import BaseStepResults
from sequence_annotation.services.domain.benchling_fields import DnaSchemaType


@dataclass
class EventValidationResult:
    event: EntityRegisteredEvent
    dna_schema_type: DnaSchemaType

    def to_dict(self) -> Dict[str, Any]:
        return {
            'event': self.event.to_dict(),
            'dna_schema_type': self.dna_schema_type
        }


@dataclass
class GetDnaSequenceResult:
    previous: EventValidationResult
    dna_sequence: DnaSequence

    @property
    def event(self) -> EntityRegisteredEvent:
        return self.previous.event

    @property
    def dna_schema_type(self) -> DnaSchemaType:
        return self.previous.dna_schema_type

    def to_dict(self) -> Dict[str, Any]:
        return {
            'previous': self.previous.to_dict(),
            'dna_sequence': self.dna_sequence.to_dict()
        }


@dataclass
class GetSpeciesResult:
    previous: GetDnaSequenceResult
    species: str

    @property
    def event(self) -> EntityRegisteredEvent:
        return self.previous.event

    @property
    def dna_sequence(self) -> DnaSequence:
        return self.previous.dna_sequence

    @property
    def dna_schema_type(self) -> DnaSchemaType:
        return self.previous.dna_schema_type

    def to_dict(self) -> Dict[str, Any]:
        return {
            'previous': self.previous.to_dict(),
            'species': self.species
        }


@dataclass_json
@dataclass
class DnaSequenceAnalysis(DataClassJsonMixin):
    imgt_version: Optional[str]
    imgt_data: Optional[Dict[str, Any]]
    kabat_data: Optional[Dict[str, Any]]


@dataclass
class IgBlastExecutionResult:
    previous: GetSpeciesResult
    analysis: DnaSequenceAnalysis

    @property
    def event(self) -> EntityRegisteredEvent:
        return self.previous.event

    @property
    def dna_sequence(self) -> DnaSequence:
        return self.previous.dna_sequence

    @property
    def dna_schema_type(self) -> DnaSchemaType:
        return self.previous.dna_schema_type

    def to_dict(self) -> Dict[str, Any]:
        return {
            'previous': self.previous.to_dict(),
            'analysis': self.analysis.to_dict()
        }


@dataclass
class CreateAaSequenceResult:
    previous: IgBlastExecutionResult
    aa_sequence_request: AaSequenceCreate | AaSequenceUpdate
    aa_sequence: AaSequence

    @property
    def event(self) -> EntityRegisteredEvent:
        return self.previous.event

    @property
    def dna_sequence(self) -> DnaSequence:
        return self.previous.dna_sequence

    @property
    def dna_schema_type(self) -> DnaSchemaType:
        return self.previous.dna_schema_type

    @property
    def analysis(self) -> DnaSequenceAnalysis:
        return self.previous.analysis

    def to_dict(self) -> Dict[str, Any]:
        return {
            'previous': self.previous.to_dict(),
            'aa_sequence_request': self.aa_sequence_request.to_dict(),
            'aa_sequence': self.aa_sequence.to_dict()
        }


@dataclass
class FillTranslationResult:
    previous: CreateAaSequenceResult

    @property
    def event(self) -> EntityRegisteredEvent:
        return self.previous.event

    @property
    def dna_sequence(self) -> DnaSequence:
        return self.previous.dna_sequence

    @property
    def analysis(self) -> DnaSequenceAnalysis:
        return self.previous.analysis

    @property
    def aa_sequence(self) -> AaSequence:
        return self.previous.aa_sequence

    def to_dict(self) -> Dict[str, Any]:
        return {
            'previous': self.previous.to_dict()
        }


@dataclass
class CreateGermlineAlignmentSequenceResult:
    previous: FillTranslationResult
    germline_alignment_sequence: DnaSequence

    @property
    def event(self) -> EntityRegisteredEvent:
        return self.previous.event

    @property
    def dna_sequence(self) -> DnaSequence:
        return self.previous.dna_sequence

    @property
    def analysis(self) -> DnaSequenceAnalysis:
        return self.previous.analysis

    @property
    def aa_sequence(self) -> AaSequence:
        return self.previous.aa_sequence

    def to_dict(self) -> Dict[str, Any]:
        return {
            'previous': self.previous.to_dict(),
            'germline_alignment_sequence': self.germline_alignment_sequence.to_dict()
        }


@dataclass
class CreateNucleotideTemplateAlignmentResult:
    previous: CreateGermlineAlignmentSequenceResult

    @property
    def event(self) -> EntityRegisteredEvent:
        return self.previous.event

    @property
    def dna_sequence(self) -> DnaSequence:
        return self.previous.dna_sequence

    @property
    def aa_sequence(self) -> AaSequence:
        return self.previous.aa_sequence

    def to_dict(self) -> Dict[str, Any]:
        return {
            'previous': self.previous.to_dict()
        }


@dataclass
class SequenceAnnotationBulkFlowResults(BaseStepResults):
    @staticmethod
    def merge(step_results: List[BaseStepResults]) -> 'SequenceAnnotationBulkFlowResults':
        results = SequenceAnnotationBulkFlowResults()

        # Get errors from all steps
        for step_result in step_results:
            for warning_result in step_result.warning:
                results.add_warning(warning_result.payload, f'{type(step_result).__name__}: {warning_result.error_message}')

            for failed_result in step_result.failed:
                results.add_failed(failed_result.payload, f'{type(step_result).__name__}: {failed_result.error_message}')

        # Get succeeded from last step
        for step_result in step_results[-1].succeeded:
            results.add_succeeded(step_result)

        return results
