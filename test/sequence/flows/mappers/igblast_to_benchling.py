import base64
from typing import Dict, Any, Optional, List

from benchling_api_client.models.custom_fields import CustomFields
from benchling_sdk.models import AaAnnotation, AaSequence, DnaSequence, Field, Fields, NamingStrategy, AaSequenceUpdate, AaSequenceCreate, DnaSequenceBulkCreate, DnaSequenceBulkUpdate, NucleotideAlignmentFile

from sequence_annotation.flows.domain.aa_regions import AaRegions
from sequence_annotation.flows.utils import sanitize_sequence
from sequence_annotation.services.benchling import BenchlingConfig
from sequence_annotation.services.domain.benchling_fields import DnaSequenceFieldKey, AaSequenceFieldKey, AaSequenceCustomFieldKey, GermlineAlignmentCustomFieldKey, DnaFeatureFieldKey, DnaSchemaType
from sequence_annotation.services.igblast import Organism

CDRS = ['cdr1', 'cdr2', 'cdr3']
FWRS = ['fwr1', 'fwr2', 'fwr3', 'fwr4']
ALIGNMENTS = ['v', 'd', 'j']

SPECIES_MAPPING = {
    'Homo sapiens': Organism.HUMAN,
    'Mus musculus': Organism.MOUSE,
    'Rattus norvegicus': Organism.RAT,
    'Macaca mulatta': Organism.RHESUS_MONKEY
    # Not supported:
    # Danio rerio
    # Drosophila melanogaster
    # Macaca fascicularis
    # Synthetic Construct
    # Xenopus
    # N/A
}


class IgBlastToBenchlingMapper:
    """
    IgBlastToBenchlingMapper class for mapping IgBlast data to Benchling payloads.
    """

    def __init__(self, benchling_config: BenchlingConfig):
        self.benchling_config = benchling_config

    def to_aa_sequence_create_or_update(self, sequence: DnaSequence, dna_schema_type: DnaSchemaType, imgt_version: str, imgt_data: Dict[str, Any], kabat_data: Dict[str, Any], existing_aa_sequence: Optional[AaSequence] = None) -> AaSequenceCreate | AaSequenceUpdate:
        """
        Builds an AA sequence payload based on the provided DNA sequence, IgBlast data and optionally, the existing AA sequence.

        Note: In the case of update, only annotations and fields are overwritten.

        :param sequence: The DNA sequence.
        :param dna_schema_type: The DNA schema type.
        :param imgt_version: The IMGT version.
        :param imgt_data: The IMGT data.
        :param kabat_data: The Kabat data.
        :param existing_aa_sequence: Optional. An existing AA sequence to update.

        :return: An instance of AaSequenceBulkCreate or AaSequenceBulkUpdate.
        """

        data_by_domain_system = {
            'IMGT': imgt_data,
            'Kabat': kabat_data
        }

        amino_acids = sanitize_sequence(imgt_data.get('sequence_alignment_aa'))

        fields = Fields()
        fields[AaSequenceFieldKey.IMGTVersion] = Field(value=imgt_version)

        if dna_schema_type == DnaSchemaType.Sequence:
            fields[AaSequenceFieldKey.SourceClone] = Field(value=sequence.fields.get(DnaSequenceFieldKey.SourceClone).value)
            fields[AaSequenceFieldKey.Type] = Field(value=sequence.fields.get(DnaSequenceFieldKey.Type).value)
        elif dna_schema_type == DnaSchemaType.Feature:
            fields[AaSequenceFieldKey.SequenceSource] = Field(value=sequence.fields.get(DnaFeatureFieldKey.SequenceSource).value)

            functions = sequence.fields.get(DnaFeatureFieldKey.Functions).value

            fields[AaSequenceFieldKey.Type] = Field(value=functions[0] if isinstance(functions, list) and len(functions) > 0 else '')

        annotations = []

        # Annotate CDRx/FWRx
        for domain_system, data in data_by_domain_system.items():
            regions = (
                AaRegions.detect(
                    amino_acids,
                    fwr1=data.get('fwr1_aa'),
                    cdr1=data.get('cdr1_aa'),
                    fwr2=data.get('fwr2_aa'),
                    cdr2=data.get('cdr2_aa'),
                    fwr3=data.get('fwr3_aa'),
                    cdr3=data.get('cdr3_aa'),
                    fwr4=data.get('fwr4_aa'),
                )
            )

            for cdr in CDRS:
                name = f'{cdr.upper()}_{domain_system}'
                value = data.get(f'{cdr}_aa')

                if isinstance(value, str):
                    fields[name] = Field(value=value)

                    region = regions.get(cdr)

                    annotations.append(AaAnnotation(color='#E2E3E5', type='Site', name=name, start=region.start, end=region.end))

            for fwr in FWRS:
                fr = fwr.replace('w', '').upper()  # fwr (IgBlast) -> fr (Benchling)
                name = f'{fr}_{domain_system}'
                value = data.get(f'{fwr}_aa')

                if isinstance(value, str):
                    fields[name] = Field(value=value)

                    region = regions.get(fwr)

                    annotations.append(AaAnnotation(color='#395EA8', type='Site', name=name, start=region.start, end=region.end))

        # Annotate alignments
        regions = (
            AaRegions.detect(
                amino_acids,
                v_alignment=sanitize_sequence(imgt_data.get(f'v_sequence_alignment_aa')),
                d_alignment=sanitize_sequence(imgt_data.get(f'd_sequence_alignment_aa')),
                j_alignment=sanitize_sequence(imgt_data.get(f'j_sequence_alignment_aa'))
            )
        )

        for alignment in ALIGNMENTS:
            name = imgt_data.get(f'{alignment}_call')
            region = regions.get(f'{alignment}_alignment')

            if region:
                annotations.append(AaAnnotation(color='#704878', type='Site', name=name, start=region.start, end=region.end))

        # If AA sequence does not exist then create else update
        if not existing_aa_sequence:
            custom_fields = CustomFields()
            custom_fields[AaSequenceCustomFieldKey.DNASequenceId] = Field(value=sequence.id)

            return AaSequenceCreate(
                name=sequence.name + '_translation',
                amino_acids=amino_acids,
                annotations=annotations,
                fields=fields,
                custom_fields=custom_fields,
                registry_id=self.benchling_config.registry_id,
                folder_id=sequence.folder_id,
                schema_id=self.benchling_config.aa_sequence_schema_id,
                naming_strategy=NamingStrategy.REPLACE_NAMES_FROM_PARTS,
                author_ids=self.benchling_config.author_ids
            )
        else:
            return AaSequenceUpdate(
                name=existing_aa_sequence.name,
                amino_acids=existing_aa_sequence.amino_acids,
                annotations=annotations,
                fields=fields,
                registry_id=existing_aa_sequence.registry_id,
                folder_id=existing_aa_sequence.folder_id,
                schema_id=existing_aa_sequence.schema.id,
                aliases=existing_aa_sequence.aliases,
                custom_fields=existing_aa_sequence.custom_fields,
                author_ids=self.benchling_config.author_ids
            )

    def to_nucleotide_alignement_file(self, sequence: DnaSequence, germline_alignment: str) -> NucleotideAlignmentFile:
        return NucleotideAlignmentFile(
            name=f'{sequence.name}_germline_alignment',
            data=base64.b64encode(germline_alignment.encode('utf-8')).decode('utf-8')
        )

    def to_dna_sequence_bulk_create(self, sequence: DnaSequence, germline_alignment: str) -> DnaSequenceBulkCreate:
        custom_fields = CustomFields()
        custom_fields[GermlineAlignmentCustomFieldKey.DNASequenceId] = Field(value=sequence.id)

        return DnaSequenceBulkCreate(
            name=f'{sequence.name}_germline_alignment',
            bases=germline_alignment,
            is_circular=True,
            folder_id=sequence.folder_id,
            custom_fields=custom_fields
        )

    def to_dna_sequence_bulk_update(self, dna_sequence: DnaSequence, computed_fields: List[str]) -> DnaSequenceBulkUpdate:
        fields = self.copy_fields(dna_sequence.fields)

        for computed_field in computed_fields:
            if computed_field in fields:
                del fields[computed_field]

        return DnaSequenceBulkUpdate(
            id=dna_sequence.id,
            name=dna_sequence.name,
            bases=dna_sequence.bases,
            fields=fields,
            custom_fields=self.copy_custom_fields(dna_sequence.custom_fields),
            folder_id=dna_sequence.folder_id,
            is_circular=dna_sequence.is_circular
        )

    def copy_fields(self, original: Fields) -> Fields:
        copy = Fields()

        for key, field in original.additional_properties.items():
            if field.value is not None:
                copy[key] = Field(value=field.value)

        return copy

    def copy_custom_fields(self, original: CustomFields) -> CustomFields:
        copy = CustomFields()

        for key, field in original.additional_properties.items():
            if field.value is not None:
                copy[key] = Field(value=field.value)

        return copy
