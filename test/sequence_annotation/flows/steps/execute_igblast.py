import itertools
import logging
from dataclasses import dataclass
from typing import List

from sequence_annotation.flows.base import BaseStep, BaseStepResults
from sequence_annotation.flows.domain.sequence_annotation import IgBlastExecutionResult, DnaSequenceAnalysis, GetSpeciesResult
from sequence_annotation.flows.mappers.igblast_to_benchling import SPECIES_MAPPING
from sequence_annotation.services.igblast import Sequence, IgBlastService, DomainSystem

logger = logging.getLogger(__name__)


@dataclass
class ExecuteIgBlastStepResults(BaseStepResults[IgBlastExecutionResult, GetSpeciesResult]):
    pass


class ExecuteIgBlastStep(BaseStep):
    __label__ = 'Execute BLASTs'

    def __init__(self, igblast_service: IgBlastService):
        self.igblast_service = igblast_service

    def execute(self, items: List[GetSpeciesResult]) -> ExecuteIgBlastStepResults:
        results = ExecuteIgBlastStepResults()

        for species, get_dna_sequence_results in itertools.groupby(items, lambda result: result.species):
            # Convert itergroup to list
            get_dna_sequence_results = list(get_dna_sequence_results)

            igblast_organism = SPECIES_MAPPING[species]
            igblast_sequences = [Sequence(result.dna_sequence.id, result.dna_sequence.bases) for result in get_dna_sequence_results]

            imgt_results = self.igblast_service.post_sequences_sync(igblast_sequences, igblast_organism, DomainSystem.IMGT)
            kabat_results = self.igblast_service.post_sequences_sync(igblast_sequences, igblast_organism, DomainSystem.KABAT)

            if imgt_results.returncode == 0 and kabat_results.returncode == 0:
                # Extract IgBlast data
                imgt_data_by_sequence_id = {
                    row.get('sequence_id'): row
                    for row in imgt_results.report.get_data()
                }

                kabat_data_by_sequence_id = {
                    row.get('sequence_id'): row
                    for row in kabat_results.report.get_data()
                }

                for item in get_dna_sequence_results:
                    results.add_succeeded(
                        IgBlastExecutionResult(
                            item,
                            DnaSequenceAnalysis(
                                imgt_results.report.imgt_version,
                                imgt_data_by_sequence_id.get(item.dna_sequence.id),
                                kabat_data_by_sequence_id.get(item.dna_sequence.id)
                            )
                        )
                    )
            else:
                for item in get_dna_sequence_results:
                    results.add_failed(item, imgt_results.error)

        return results
