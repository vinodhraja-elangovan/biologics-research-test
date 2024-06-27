import logging
from dataclasses import dataclass
from typing import List

from benchling_sdk.models import BulkUpdateDnaSequencesAsyncTask, DnaSequence, DnaSequenceBulkUpdate, AaSequence, Field

from sequence_annotation.flows.base import BaseStepResults, OUTPUT
from sequence_annotation.flows.domain.sequence_annotation import FillTranslationResult, CreateAaSequenceResult
from sequence_annotation.flows.mappers.igblast_to_benchling import IgBlastToBenchlingMapper
from sequence_annotation.flows.steps.base import BaseAsyncTaskStep
from sequence_annotation.services.benchling import BenchlingService
from sequence_annotation.services.domain.benchling_fields import DnaSequenceFieldKey, DnaSchemaType, DnaFeatureFieldKey

logger = logging.getLogger(__name__)


@dataclass
class FillTranslationsStepResults(BaseStepResults[FillTranslationResult, CreateAaSequenceResult]):
    pass


class FillTranslationsStep(BaseAsyncTaskStep[CreateAaSequenceResult, FillTranslationsStepResults]):
    __label__ = 'Fill DNA sequence translations'

    def __init__(self, benchling_service: BenchlingService, mapper: IgBlastToBenchlingMapper):
        self.benchling_service = benchling_service
        self.mapper = mapper

    def initialize_results(self) -> FillTranslationsStepResults:
        return FillTranslationsStepResults()

    def get_task(self, items: List[CreateAaSequenceResult]) -> BulkUpdateDnaSequencesAsyncTask:
        return self.benchling_service.update_dna_sequences([
            self._to_dna_sequence_bulk_update(result.dna_sequence, result.dna_schema_type, result.aa_sequence)
            for result in items
        ])

    def get_succeeded_items(self, items: List[CreateAaSequenceResult], task: BulkUpdateDnaSequencesAsyncTask) -> List[OUTPUT]:
        return [FillTranslationResult(item) for item in items]

    def _to_dna_sequence_bulk_update(self, dna_sequence: DnaSequence, dna_schema_type: DnaSchemaType, aa_sequence: AaSequence) -> DnaSequenceBulkUpdate:
        payload = self.mapper.to_dna_sequence_bulk_update(dna_sequence, dna_schema_type.get_computed_fields())

        if dna_schema_type == DnaSchemaType.Sequence:
            payload.fields[DnaSequenceFieldKey.TranslationAA] = Field(value=[aa_sequence.id])
        elif dna_schema_type == DnaSchemaType.Feature:
            payload.fields[DnaFeatureFieldKey.TranslationAA] = Field(value=aa_sequence.id)

        return payload
