import logging
from dataclasses import dataclass
from typing import List, Optional, Dict

from benchling_sdk.models import CustomEntity, DnaSequence

from sequence_annotation.flows.base import BaseStep, BaseStepResults
from sequence_annotation.flows.domain.sequence_annotation import GetDnaSequenceResult, GetSpeciesResult
from sequence_annotation.flows.mappers.igblast_to_benchling import SPECIES_MAPPING
from sequence_annotation.services.benchling import BenchlingService
from sequence_annotation.services.domain.benchling_fields import DnaSequenceFieldKey, DnaSchemaType, DnaFeatureFieldKey

logger = logging.getLogger(__name__)


@dataclass
class GetSpeciesStepResults(BaseStepResults[GetSpeciesResult, GetDnaSequenceResult]):
    pass


class GetSpeciesStep(BaseStep):
    __label__ = 'Filter events'

    def __init__(self, benchling_service: BenchlingService):
        self.benchling_service = benchling_service

    def execute(self, items: List[GetDnaSequenceResult]) -> GetSpeciesStepResults:
        results = GetSpeciesStepResults()

        if len(items) == 0:
            return results

        species_by_dna_sequence_id = self._get_species_by_dna_sequence_id(items)

        for item in items:
            species = species_by_dna_sequence_id.get(item.dna_sequence.id)

            if species not in SPECIES_MAPPING:
                results.add_failed(item, f'Species "{species}" not supported')
            else:
                results.add_succeeded(GetSpeciesResult(item, species))

        return results

    def _get_field_text_value(self, entity: DnaSequence | CustomEntity, key: str) -> Optional[str]:
        return entity.fields.get(key).text_value if key in entity.fields else None

    def _get_species_by_dna_sequence_id(self, items: List[GetDnaSequenceResult]) -> Dict[str, str]:
        # Retrieve species from DNA features
        species_by_dna_sequence_id = {
            item.dna_sequence.id: self._get_field_text_value(item.dna_sequence, DnaFeatureFieldKey.SourceSpecies)
            for item in items
            if item.dna_schema_type == DnaSchemaType.Feature
        }

        # Group "Source: Clone" entity ID by DNA sequence ID
        custom_entity_id_by_dna_sequence_id = {
            item.dna_sequence.id: item.dna_sequence.fields.get(DnaSequenceFieldKey.SourceClone).value
            for item in items
            if item.dna_schema_type == DnaSchemaType.Sequence
        }

        # Make custom entity ids unique
        unique_custom_entity_ids = list(set(custom_entity_id_by_dna_sequence_id.values()))

        if len(unique_custom_entity_ids) > 0:
            # Retrieve custom entities and only includes ID and species
            custom_entities = self.benchling_service.get_custom_entities_by_id(unique_custom_entity_ids)

            # Group species by custom entity ID
            species_by_custom_entity_id = {
                custom_entity.id: self._get_field_text_value(custom_entity, DnaSequenceFieldKey.Species)
                for custom_entity in custom_entities
            }

            for dna_sequence_id in custom_entity_id_by_dna_sequence_id.keys():
                custom_entity_id = custom_entity_id_by_dna_sequence_id.get(dna_sequence_id)
                species = species_by_custom_entity_id.get(custom_entity_id)

                species_by_dna_sequence_id[dna_sequence_id] = species

        return species_by_dna_sequence_id
