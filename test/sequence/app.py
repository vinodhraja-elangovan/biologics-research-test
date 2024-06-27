import itertools
import json
import logging
from typing import Any, Dict

from benchling_sdk.models import EntityRegisteredEvent

from sequence_annotation.flows.sequence_annotation import SequenceAnnotationBulkFlow
from sequence_annotation.services.benchling import BenchlingConfig, BenchlingService
from sequence_annotation.services.igblast import IgBlastConfig, IgBlastService

logger = logging.getLogger(__name__)


def lambda_handler(event: Dict[str, Any], context):
    logger.info('Event received', extra={'event': event})

    benchling_config = BenchlingConfig.read_from_env()
    benchling_service = BenchlingService(benchling_config)
    igblast_service = IgBlastService(IgBlastConfig.read_from_env())

    events = [
        EntityRegisteredEvent.from_dict(json.loads(record.get('body')).get('detail'))
        for record in event.get('Records')
    ]

    try:
        for schema_id, events in itertools.groupby(events, lambda e: e.schema.id):
            if schema_id == benchling_config.dna_sequence_schema_id or schema_id == benchling_config.dna_feature_schema_id:  # DNA sequence
                flow = SequenceAnnotationBulkFlow(benchling_service, igblast_service)
                flow.execute(list(events))
            else:
                logger.error(f'Unsupported schema "{schema_id}"')
    except:
        logger.exception('Unexpected error while running lambda')
