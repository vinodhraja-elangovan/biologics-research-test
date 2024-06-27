import logging
from dataclasses import dataclass
from typing import List

from sequence_annotation.flows.base import BaseStep, BaseStepResults
from sequence_annotation.flows.domain.sequence_annotation import IgBlastExecutionResult, CreateAaSequenceResult
from sequence_annotation.flows.mappers.igblast_to_benchling import IgBlastToBenchlingMapper
from sequence_annotation.services.benchling import BenchlingService

logger = logging.getLogger(__name__)


@dataclass
class CreateAaSequencesStepResults(BaseStepResults[CreateAaSequenceResult, IgBlastExecutionResult]):
    pass


class CreateAaSequencesStep(BaseStep):
    __label__ = 'Create AA sequences'

    def __init__(self, benchling_service: BenchlingService, mapper: IgBlastToBenchlingMapper):
        self.benchling_service = benchling_service
        self.mapper = mapper

    def execute(self, items: List[IgBlastExecutionResult]) -> CreateAaSequencesStepResults:
        results = CreateAaSequencesStepResults()

        if len(items) == 0:
            return results

        for item in items:
            try:
                request = self.mapper.to_aa_sequence_create_or_update(
                    item.dna_sequence,
                    item.dna_schema_type,
                    item.analysis.imgt_version,
                    item.analysis.imgt_data,
                    item.analysis.kabat_data
                )

                aa_sequence = self.benchling_service.create_aa_sequence(request)

                results.add_succeeded(CreateAaSequenceResult(item, request, aa_sequence))
            except Exception as e:
                logger.exception('Error while creating or updating AA sequence')

                results.add_failed(item, str(e))

        return results
