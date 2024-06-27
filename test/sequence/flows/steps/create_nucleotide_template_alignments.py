import logging
from dataclasses import dataclass
from typing import List

from sequence_annotation.flows.base import BaseStep, BaseStepResults
from sequence_annotation.flows.domain.sequence_annotation import CreateNucleotideTemplateAlignmentResult, CreateGermlineAlignmentSequenceResult
from sequence_annotation.services.benchling import BenchlingService

logger = logging.getLogger(__name__)


@dataclass
class CreateNucleotideTemplateAlignmentStepResults(BaseStepResults[CreateNucleotideTemplateAlignmentResult, CreateGermlineAlignmentSequenceResult]):
    pass


class CreateNucleotideTemplateAlignmentStep(BaseStep):
    __label__ = 'Create nucleotide template alignments'

    def __init__(self, benchling_service: BenchlingService):
        self.benchling_service = benchling_service

    def execute(self, items: List[CreateGermlineAlignmentSequenceResult]) -> CreateNucleotideTemplateAlignmentStepResults:
        results = CreateNucleotideTemplateAlignmentStepResults()

        if len(items) == 0:
            return results

        for item in items:
            try:
                self.benchling_service.create_template_alignment(item.germline_alignment_sequence.name, item.dna_sequence.id, item.germline_alignment_sequence.id)

                results.add_succeeded(CreateNucleotideTemplateAlignmentResult(item))
            except Exception as e:
                logger.exception('Error while creating nucleotide template alignment')

                results.add_failed(item, str(e))

        return results
