import dataclasses
import logging
import os
import time
from typing import List, Optional

import httpx
from benchling_sdk.auth.client_credentials_oauth2 import ClientCredentialsOAuth2
from benchling_sdk.benchling import Benchling
from benchling_sdk.models import NucleotideAlignmentBaseAlgorithm, AppSessionCreate, AppSessionUpdate, AppSessionMessageStyle, AppSessionMessageCreate, AppSessionUpdateStatus, \
    AsyncTaskLink, DnaSequence, AaSequence, AaSequenceCreate, AaSequenceUpdate, NucleotideTemplateAlignmentCreate, NucleotideAlignmentBaseFilesItem, AaSequenceBulkCreate, AsyncTask, DnaSequenceBulkCreate, BulkCreateDnaSequencesAsyncTask, \
    DnaSequenceBulkUpdate, BulkUpdateDnaSequencesAsyncTask, CustomEntity
from httpx import Response

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class BenchlingConfig:
    url: str
    app_id: str
    client_id: str
    client_secret: str
    registry_id: str
    dna_sequence_schema_id: str
    dna_feature_schema_id: str
    aa_sequence_schema_id: str
    author_ids: List[str]

    @staticmethod
    def read_from_env() -> 'BenchlingConfig':
        return BenchlingConfig(
            url=BenchlingConfig.getenv('BENCHLING_URL'),
            app_id=BenchlingConfig.getenv('BENCHLING_APP_ID'),
            client_id=BenchlingConfig.getenv('BENCHLING_CLIENT_ID'),
            client_secret=BenchlingConfig.getenv('BENCHLING_CLIENT_SECRET'),
            registry_id=BenchlingConfig.getenv('BENCHLING_REGISTRY_ID'),
            dna_sequence_schema_id=BenchlingConfig.getenv('BENCHLING_DNA_SEQUENCE_SCHEMA_ID'),
            dna_feature_schema_id=BenchlingConfig.getenv('BENCHLING_DNA_FEATURE_SCHEMA_ID'),
            aa_sequence_schema_id=BenchlingConfig.getenv('BENCHLING_AA_SEQUENCE_SCHEMA_ID'),
            author_ids=[BenchlingConfig.getenv('BENCHLING_AUTHOR_ID')]
        )

    @staticmethod
    def getenv(key: str, required=True, default=None) -> str:
        value = os.environ[key]

        if value is None:
            if required:
                raise EnvironmentError(f'Missing {key} environment variable')
            else:
                return default

        return value


class CustomClient(httpx.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.timeout = 15
        self.rate_limit_threshold = .25

    def send(self, *args, **kwargs) -> Response:
        response = super().send(*args, **kwargs)

        rate_limit_limit = int(response.headers.get('x-rate-limit-limit'))  # The total number of requests allowed per period
        rate_limit_remaining = int(response.headers.get('x-rate-limit-remaining'))  # The number of requests remaining per period
        rate_limit_reset = int(response.headers.get('x-rate-limit-reset'))  # The number of seconds remaining in the period

        logger.info(f'Rate limit: {rate_limit_remaining}/{rate_limit_limit} - {rate_limit_reset}s left for reset')

        # If remaining number of requests is too low (below threshold) then wait till it reset
        if rate_limit_remaining <= rate_limit_limit * self.rate_limit_threshold:
            logger.info(f'Waiting {rate_limit_reset}s for rate limit reset')
            time.sleep(rate_limit_reset)

        return response


class BenchlingService:
    def __init__(self, config: BenchlingConfig):
        self.config = config

        auth_method = ClientCredentialsOAuth2(client_id=config.client_id, client_secret=config.client_secret)
        httpx_client = CustomClient()
        self.benchling = Benchling(url=config.url, auth_method=auth_method, httpx_client=httpx_client)

        logger.info('Benchling configured', extra={'url': config.url})

    def create_session(self, name: str, timeout_seconds=3600) -> str:
        request = AppSessionCreate(
            app_id=self.config.app_id,
            name=name,
            timeout_seconds=timeout_seconds
        )

        logger.info('Creating app session', extra={'request': request.to_dict()})

        response = self.benchling.apps.create_session(request)

        logger.info('App session created', extra={'response': response.to_dict()})

        return response.id

    def add_session_message(self, session_id: str, content: str, style: AppSessionMessageStyle) -> None:
        # truncate content if larger than maximum
        if len(content) > 200:
            content = content[:197] + '...'

        request = AppSessionUpdate(messages=[
            AppSessionMessageCreate(content=content, style=style)
        ])

        logger.debug('Adding session message', extra={'request': request.to_dict()})

        response = self.benchling.apps.update_session(session_id, request)

        logger.debug('Session message added', extra={'response': response.to_dict()})

    def update_session_status(self, session_id: str, status: AppSessionUpdateStatus) -> None:
        request = AppSessionUpdate(status=status)

        logger.debug('Updating session status', extra={'request': request.to_dict()})

        response = self.benchling.apps.update_session(session_id, request)

        logger.debug('Session status updated', extra={'response': response.to_dict()})

    def create_dna_sequences(self, dna_sequences: List[DnaSequenceBulkCreate]) -> BulkCreateDnaSequencesAsyncTask:
        logger.info('Create DNA sequences create task into Benchling', extra={'request': [sequence.to_dict() for sequence in dna_sequences]})

        async_task_link = self.benchling.dna_sequences.bulk_create(dna_sequences)

        task = self.wait_for_task(async_task_link.task_id)
        task = BulkCreateDnaSequencesAsyncTask.from_dict(task.to_dict())

        logger.info('DNA sequences create task retrieved from Benchling', extra={'response': task.to_dict()})

        return task

    def update_dna_sequences(self, dna_sequences: List[DnaSequenceBulkUpdate]) -> BulkUpdateDnaSequencesAsyncTask:
        logger.info('Create DNA sequences update task into Benchling', extra={'request': [sequence.to_dict() for sequence in dna_sequences]})

        async_task_link = self.benchling.dna_sequences.bulk_update(dna_sequences)

        task = self.wait_for_task(async_task_link.task_id)
        task = BulkUpdateDnaSequencesAsyncTask.from_dict(task.to_dict())

        logger.info('DNA sequences update task retrieved from Benchling', extra={'response': task.to_dict()})

        return task

    def get_dna_sequence_by_id(self, sequence_id: str) -> DnaSequence:
        logger.info('Get DNA sequence from Benchling', extra={'request': sequence_id})

        sequence = self.benchling.dna_sequences.get_by_id(sequence_id)

        logger.info('DNA sequence retrieved from Benchling', extra={'response': sequence.to_dict()})

        return sequence

    def get_dna_sequences_by_id(self, sequence_ids: List[str]) -> List[DnaSequence]:
        logger.info('Get DNA sequences from Benchling', extra={'request': sequence_ids})

        sequences = self.benchling.dna_sequences.bulk_get(sequence_ids)

        logger.info('DNA sequences retrieved from Benchling', extra={'response': [sequence.to_dict() for sequence in sequences]})

        return sequences

    def get_custom_entities_by_id(self, entity_ids: List[str], returning: Optional[List[str]] = None) -> List[CustomEntity]:
        logger.info('Get custom entities from Benchling', extra={'request': {'entity_ids': entity_ids, 'returning': returning}})

        custom_entities = self.benchling.custom_entities.bulk_get(entity_ids)

        logger.info('Custom entities retrieved from Benchling', extra={'response': [custom_entity.to_dict() for custom_entity in custom_entities]})

        return custom_entities

    def create_aa_sequence(self, request: AaSequenceCreate) -> AaSequence:
        logger.info('Creating AA sequence into Benchling', extra={'request': request.to_dict()})

        sequence = self.benchling.aa_sequences.create(request)

        logger.info('AA sequence created', extra={'response': sequence.to_dict()})

        return sequence

    def bulk_create_aa_sequences(self, request: List[AaSequenceBulkCreate]) -> AsyncTaskLink:
        logger.info('Creating AA sequences creation task into Benchling', extra={'request': [item.to_dict() for item in request]})

        response = self.benchling.aa_sequences.bulk_create(request)

        logger.info('AA sequences task creation created into Benchling', extra={'response': response.to_dict()})

        return response

    def get_aa_sequence(self, sequence_id: str) -> AaSequence:
        logger.info('Get AA sequence from Benchling', extra={'request': sequence_id})

        sequence = self.benchling.aa_sequences.get_by_id(sequence_id)

        logger.info('AA sequence retrieved from Benchling', extra={'response': sequence.to_dict()})

        return sequence

    def update_aa_sequence(self, sequence_id: str, request: AaSequenceUpdate) -> AaSequence:
        logger.info('Update AA sequence in Benchling', extra={'request': request})

        sequence = self.benchling.aa_sequences.update(sequence_id, request)

        logger.info('AA sequence updated', extra={'response': sequence.to_dict()})

        return sequence

    def autofill_translations(self, sequence_ids: List[str]) -> AsyncTaskLink:
        logger.info('Creating autofilling DNA sequence translations task into Benchling', extra={'request': sequence_ids})

        response = self.benchling.dna_sequences.autofill_translations(sequence_ids)

        logger.info('Autofilling DNA sequence translations task created into Benchling', extra={'response': response.to_dict()})

        return response

    def create_template_alignment(self, name: str, sequence_id: str, germline_alignment_sequence_id: str) -> AsyncTaskLink:
        request = NucleotideTemplateAlignmentCreate(
            algorithm=NucleotideAlignmentBaseAlgorithm.MAFFT,
            files=[
                NucleotideAlignmentBaseFilesItem(sequence_id=sequence_id),
                NucleotideAlignmentBaseFilesItem(sequence_id=germline_alignment_sequence_id)
            ],
            name=name,
            template_sequence_id=sequence_id
        )

        logger.info('Creating nucleotide template alignment task into Benchling', extra={'request': request.to_dict()})

        response = self.benchling.nucleotide_alignments.create_template_alignment(request)

        logger.info('Nucleotide template alignment task created into Benchling', extra={'response': response.to_dict()})

        return response

    def get_task(self, task_id: str) -> AsyncTask:
        logger.info('Get task from Benchling', extra={'request': task_id})

        task = self.benchling.tasks.get_by_id(task_id)

        logger.info('Task retrieved from Benchling', extra={'request': task.to_dict()})

        return task

    def wait_for_task(self, task_id: str, interval_wait_seconds: int = 1, max_wait_seconds: int = 30) -> AsyncTask:
        logger.info('Wait for task from Benchling', extra={'request': task_id})

        task = self.benchling.tasks.wait_for_task(task_id, interval_wait_seconds, max_wait_seconds)

        logger.info('Task retrieved from Benchling', extra={'request': task.to_dict()})

        return task


class BenchlingAppSession:
    def __init__(self, benchling_service: BenchlingService, name: str):
        self.benchling_service = benchling_service
        self.name = name
        self.session_id = None
        self.session_status = AppSessionUpdateStatus.SUCCEEDED

    def __enter__(self):
        self.session_id = self.benchling_service.create_session(self.name)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.benchling_service.add_session_message(self.session_id, str(exc_val), AppSessionMessageStyle.ERROR)

        self.benchling_service.update_session_status(self.session_id, self.session_status if exc_type is None else AppSessionUpdateStatus.FAILED)

        # Return False to propagate the exception, True to suppress
        return exc_type is None

    def add_message(self, content: str, style: Optional[AppSessionMessageStyle] = None):
        if style is None:
            style = AppSessionMessageStyle.INFO

        self.benchling_service.add_session_message(self.session_id, content, style)
