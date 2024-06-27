import logging
from dataclasses import dataclass
from typing import List, Optional

from benchling_api_client.v2.stable.models.entity_registered_event import EntityRegisteredEvent
from benchling_sdk.models import GenericEntity

from sequence_annotation.flows.base import BaseStep, BaseStepResults
from sequence_annotation.flows.domain.sequence_annotation import EventValidationResult
from sequence_annotation.services.benchling import BenchlingConfig
from sequence_annotation.services.domain.benchling_fields import DnaSequenceFieldKey, DnaSchemaType, DnaFeatureFieldKey

logger = logging.getLogger(__name__)

SUPPORTED_TYPES = ['VL', 'VH', 'HCDR3']


@dataclass
class ValidateEventsStepResults(BaseStepResults[EventValidationResult, EventValidationResult]):
    pass


class ValidateEventsStep(BaseStep):
    __label__ = 'Filter events'

    def __init__(self, benchling_config: BenchlingConfig):
        self.benchling_config = benchling_config

    def execute(self, items: List[EntityRegisteredEvent]) -> ValidateEventsStepResults:
        results = ValidateEventsStepResults()

        for item in items:
            entity = item.entity
            dna_schema_type = self._get_dna_schema_type(entity)
            error_message = self._get_error_message(entity, dna_schema_type)

            if error_message:
                logger.error(f'{error_message} [id={entity.id}, name={entity.name}]')
                results.add_failed(EventValidationResult(item, dna_schema_type), error_message)
            else:
                results.add_succeeded(EventValidationResult(item, dna_schema_type))

        return results

    def _get_field_value(self, entity: GenericEntity, key: str) -> Optional[str]:
        return entity.fields.get(key).value if key in entity.fields else None

    def _get_field_text_value(self, entity: GenericEntity, key: str) -> Optional[str]:
        return entity.fields.get(key).text_value if key in entity.fields else None

    def _get_dna_schema_type(self, entity: GenericEntity) -> Optional[DnaSchemaType]:
        if entity.schema.id == self.benchling_config.dna_sequence_schema_id:
            return DnaSchemaType.Sequence
        elif entity.schema.id == self.benchling_config.dna_feature_schema_id:
            return DnaSchemaType.Feature
        else:
            return None

    def _get_error_message(self, entity: GenericEntity, dna_schema_type: Optional[DnaSchemaType]) -> Optional[str]:
        if dna_schema_type == DnaSchemaType.Sequence:
            type = self._get_field_text_value(entity, DnaSequenceFieldKey.Type)
            source_clone = self._get_field_value(entity, DnaSequenceFieldKey.SourceClone)

            if type not in SUPPORTED_TYPES:
                return f'Type "{type}" is not supported'
            elif not source_clone:
                return f'Missing "{DnaSequenceFieldKey.SourceClone}" field value'
            else:
                return None
        elif dna_schema_type == DnaSchemaType.Feature:
            functions = self._get_field_text_value(entity, DnaFeatureFieldKey.Functions)

            if functions not in SUPPORTED_TYPES:
                return f'Type "{functions}" is not supported'
            else:
                return None
        else:
            return f'Unknown entity schema "{entity.schema.id}"'
