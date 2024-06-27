import logging
from dataclasses import dataclass
from typing import List

from sequence_annotation.flows.base import BaseStep, BaseStepResults
from sequence_annotation.flows.domain.sequence_annotation import GetDnaSequenceResult, EventValidationResult
from sequence_annotation.services.benchling import BenchlingService

logger = logging.getLogger(__name__)


@dataclass
class GetDnaSequencesStepResults(BaseStepResults[GetDnaSequenceResult, EventValidationResult]):
    pass


class GetDnaSequencesStep(BaseStep):
    __label__ = 'Get DNA sequences'

    def __init__(self, benchling_service: BenchlingService):
        self.benchling_service = benchling_service

    def execute(self, items: List[EventValidationResult]) -> GetDnaSequencesStepResults:
        results = GetDnaSequencesStepResults()

        if len(items) == 0:
            return results

        dna_sequence_ids = [item.event.entity.id for item in items]
        dna_sequences = self.benchling_service.get_dna_sequences_by_id(dna_sequence_ids)

        dna_sequence_by_id = {dna_sequence.id: dna_sequence for dna_sequence in dna_sequences}

        for item in items:
            results.add_succeeded(GetDnaSequenceResult(item, dna_sequence_by_id.get(item.event.entity.id)))

        return results
