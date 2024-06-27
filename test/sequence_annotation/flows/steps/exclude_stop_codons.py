import logging
from dataclasses import dataclass
from typing import List, Optional

from benchling_sdk.models import GenericEntity, AppSessionMessageStyle, DnaSequenceBulkUpdate, DnaSequence, Field

from sequence_annotation.flows.base import BaseStep, BaseStepResults
from sequence_annotation.flows.domain.sequence_annotation import GetDnaSequenceResult, IgBlastExecutionResult
from sequence_annotation.flows.mappers.igblast_to_benchling import IgBlastToBenchlingMapper
from sequence_annotation.services.benchling import BenchlingAppSession, BenchlingService
from sequence_annotation.services.domain.benchling_fields import DnaSequenceFieldKey, DnaSchemaType, DnaFeatureFieldKey

logger = logging.getLogger(__name__)


@dataclass
class ExcludeStopCodonsStepResults(BaseStepResults[IgBlastExecutionResult, IgBlastExecutionResult]):
    pass


class ExcludeStopCodonsStep(BaseStep):
    __label__ = 'Exclude sequences with stop codon'

    def __init__(self, benchling_service: BenchlingService, session: BenchlingAppSession, mapper: IgBlastToBenchlingMapper):
        self.benchling_service = benchling_service
        self.session = session
        self.mapper = mapper

    def execute(self, items: List[IgBlastExecutionResult]) -> ExcludeStopCodonsStepResults:
        results = ExcludeStopCodonsStepResults()

        excluded_dna_sequences = []

        for item in items:
            if item.analysis.imgt_data.get('stop_codon') != 'T':
                results.add_succeeded(item)
            else:
                results.add_warning(item, 'Non-productive sequence: stop codon')

                self.session.add_message(f'Non-productive sequence: stop codon for {{id:{item.dna_sequence.id}}}', AppSessionMessageStyle.WARNING)

                excluded_dna_sequences.append(item)

        if len(excluded_dna_sequences) > 0:
            self.benchling_service.update_dna_sequences([
                self._to_dna_sequence_bulk_update(item.dna_sequence, item.dna_schema_type)
                for item in excluded_dna_sequences
            ])

        return results

    def _get_field_value(self, entity: GenericEntity, key: str) -> Optional[str]:
        return entity.fields.get(key).value if key in entity.fields else None

    def _get_field_text_value(self, entity: GenericEntity, key: str) -> Optional[str]:
        return entity.fields.get(key).text_value if key in entity.fields else None

    def _to_dna_sequence_bulk_update(self, dna_sequence: DnaSequence, dna_schema_type: DnaSchemaType) -> DnaSequenceBulkUpdate:
        payload = self.mapper.to_dna_sequence_bulk_update(dna_sequence, dna_schema_type.get_computed_fields())

        comment_field = Field(value='Non-productive sequence: stop codon')

        if dna_schema_type == DnaSchemaType.Sequence:
            payload.fields[DnaSequenceFieldKey.Comment] = comment_field
        elif dna_schema_type == DnaSchemaType.Feature:
            payload.fields[DnaFeatureFieldKey.Comment] = comment_field

        return payload
