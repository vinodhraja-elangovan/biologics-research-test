import dataclasses
import enum
import json
import logging
import os
from io import BytesIO, StringIO
from typing import Any, Dict, Optional, List

import dataclasses_json
import pandas as pd
import requests

logger = logging.getLogger(__name__)


class Organism(enum.Enum):
    HUMAN = 'human'
    MOUSE = 'mouse'
    RAT = 'rat'
    RABBIT = 'rabbit'
    RHESUS_MONKEY = 'rhesus_monkey'


class DomainSystem(enum.Enum):
    IMGT = 'imgt'
    KABAT = 'kabat'


@dataclasses_json.dataclass_json
@dataclasses.dataclass
class Sequence:
    id: str
    bases: str

    def __str__(self):
        return f'>{self.id}\n{self.bases}'


@dataclasses.dataclass
class PostSequencesRequest:
    sequences: List[Sequence]
    organism: Organism
    domain_system: DomainSystem

    def _get_cli_args(self):
        return [
            '-germline_db_V', f'imgt_{self.organism.value}_IGV',
            '-germline_db_D', f'imgt_{self.organism.value}_IGD',
            '-germline_db_J', f'imgt_{self.organism.value}_IGJ',
            '-auxiliary_data', f'optional_file/{self.organism.value}_gl.aux',
            '-organism', self.organism.value,
            '-query', '@file',
            '-show_translation',
            '-outfmt', '19',
            '-domain_system', self.domain_system.value,
            '-ig_seqtype', 'Ig',
            '-num_alignments_V', '1',
            '-num_alignments_D', '1',
            '-num_alignments_J', '1',
            '-strand', 'both',
            '-extend_align5end',
            '-extend_align3end'
        ]

    def to_files(self):
        request = {
            'args': self._get_cli_args()
        }

        sequences = '\n'.join(str(sequence) for sequence in self.sequences)
        sequence_bytes = BytesIO(sequences.encode('utf-8'))

        return {
            'request_json': (None, json.dumps(request), 'application/json'),
            'file': ('file', sequence_bytes)
        }


@dataclasses.dataclass
class IgBlastCommandResults:
    data: str
    imgt_version: Optional[str]

    def get_data(self) -> List[Dict[str, Any]]:
        df = pd.read_csv(StringIO(self.data), sep='\t')
        return df.to_dict(orient='records')

    def to_dict(self):
        return {
            'data': self.get_data(),
            'imgt_version': self.imgt_version
        }


@dataclasses.dataclass
class GetResultResponse:
    key: str
    process_time: float
    returncode: int
    error: str
    report: IgBlastCommandResults

    def to_dict(self):
        return {
            'key': self.key,
            'process_time': self.process_time,
            'returncode': self.returncode,
            'error': self.error,
            'report': self.report.to_dict()
        }


class ApiException(Exception):
    pass


@dataclasses.dataclass
class IgBlastConfig:
    url: str

    @staticmethod
    def read_from_env() -> 'IgBlastConfig':
        url = os.environ['IGBLAST_URL']

        return IgBlastConfig(url=url)


class IgBlastService:
    def __init__(self, config: IgBlastConfig):
        self.config = config

    def post_sequences_sync(self, sequences: List[Sequence], organism: Organism, domain_system: DomainSystem) -> GetResultResponse:
        key = self.post_sequences(PostSequencesRequest(sequences, organism, domain_system))

        results = self.get_results(key, True)

        if results.returncode != 0:
            logger.error(f'Error with IgBlast command for {domain_system.name} domain system [sequences={sequences}]')

        return results

    def post_sequences(self, request: PostSequencesRequest) -> str:
        files = request.to_files()

        logger.info('Executing IgBlast command', extra={'request': files.get('request_json'), 'sequence': [sequence.to_dict() for sequence in request.sequences]})

        response = requests.post(f'{self.config.url}/commands/igblastn', files=files)
        payload = response.json()

        key = payload.get('key')

        if key is None:
            logger.info('Unable to retrieve key', extra={'payload': payload})

            raise ApiException('Key is not set')

        logger.info('IgBlast command executed', extra={'key': key})

        return key

    def get_results(self, key: str, wait: bool) -> GetResultResponse:
        logger.info('Retrieving IgBlast command results', extra={'key': key, 'wait': wait})

        response = requests.get(f'{self.config.url}/commands/igblastn?key={key}&wait={str(wait).lower()}')
        response_json = response.json()

        report = response_json.get('report')

        if report and isinstance(report, dict):
            cmd_results = IgBlastCommandResults(
                report.get('data'),
                report.get('imgt_version')
            )
        else:
            cmd_results = IgBlastCommandResults(str(report), None)

        response = GetResultResponse(
            response_json.get('key'),
            response_json.get('process_time'),
            response_json.get('returncode'),
            response_json.get('error'),
            cmd_results
        )

        logger.info('IgBlast command results retrieved', extra={'results': response.to_dict()})

        return response
