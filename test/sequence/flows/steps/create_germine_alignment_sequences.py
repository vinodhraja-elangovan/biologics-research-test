import logging
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

from benchling_sdk.models import BulkCreateDnaSequencesAsyncTask

from sequence_annotation.flows.base import BaseStepResults
from sequence_annotation.flows.domain.sequence_annotation import CreateGermlineAlignmentSequenceResult, FillTranslationResult
from sequence_annotation.flows.mappers.igblast_to_benchling import IgBlastToBenchlingMapper
from sequence_annotation.flows.steps.base import BaseAsyncTaskStep
from sequence_annotation.flows.utils import sanitize_sequence
from sequence_annotation.services.benchling import BenchlingService
from sequence_annotation.services.domain.benchling_fields import GermlineAlignmentCustomFieldKey

logger = logging.getLogger(__name__)


@dataclass
class CreateGermlineAlignmentSequencesStepResults(BaseStepResults[CreateGermlineAlignmentSequenceResult, FillTranslationResult]):
    pass


class CreateGermlineAlignmentSequencesStep(BaseAsyncTaskStep[FillTranslationResult, CreateGermlineAlignmentSequencesStepResults]):
    __label__ = 'Create germline alignment sequences'

    def __init__(self, benchling_service: BenchlingService, mapper: IgBlastToBenchlingMapper):
        self.benchling_service = benchling_service
        self.mapper = mapper

    def initialize_results(self) -> CreateGermlineAlignmentSequencesStepResults:
        return CreateGermlineAlignmentSequencesStepResults()

    def get_task(self, items: List[FillTranslationResult]) -> BulkCreateDnaSequencesAsyncTask:
        return self.benchling_service.create_dna_sequences([
            self.mapper.to_dna_sequence_bulk_create(item.dna_sequence, sanitize_sequence(item.analysis.imgt_data.get('germline_alignment')))
            for item in items
        ])

    def get_succeeded_items(self, items: List[FillTranslationResult], task: BulkCreateDnaSequencesAsyncTask) -> List[CreateGermlineAlignmentSequenceResult]:
        germline_sequence_by_sequence_id = {
            sequence.custom_fields.get(GermlineAlignmentCustomFieldKey.DNASequenceId).value: sequence
            for sequence in task.response.dna_sequences
        }

        return [CreateGermlineAlignmentSequenceResult(item, germline_sequence_by_sequence_id.get(item.dna_sequence.id)) for item in items]
