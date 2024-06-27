import logging
from typing import List, Tuple

from benchling_sdk.models import AppSessionMessageStyle, EntityRegisteredEvent, AppSessionUpdateStatus

from sequence_annotation.flows.base import BaseFlow, StepFailedResult
from sequence_annotation.flows.domain.sequence_annotation import SequenceAnnotationBulkFlowResults
from sequence_annotation.flows.mappers.igblast_to_benchling import IgBlastToBenchlingMapper
from sequence_annotation.flows.steps.create_germine_alignment_sequences import CreateGermlineAlignmentSequencesStep
from sequence_annotation.flows.steps.create_nucleotide_template_alignments import CreateNucleotideTemplateAlignmentStep
from sequence_annotation.flows.steps.create_aa_sequences import CreateAaSequencesStep
from sequence_annotation.flows.steps.exclude_stop_codons import ExcludeStopCodonsStep
from sequence_annotation.flows.steps.execute_igblast import ExecuteIgBlastStep
from sequence_annotation.flows.steps.fill_translations import FillTranslationsStep
from sequence_annotation.flows.steps.get_dna_sequences import GetDnaSequencesStep
from sequence_annotation.flows.steps.get_species import GetSpeciesStep
from sequence_annotation.flows.steps.validate_events import ValidateEventsStep
from sequence_annotation.services.benchling import BenchlingService, BenchlingAppSession
from sequence_annotation.services.igblast import IgBlastService

logger = logging.getLogger(__name__)


class SequenceAnnotationBulkFlow(BaseFlow[List[EntityRegisteredEvent], SequenceAnnotationBulkFlowResults]):
    def __init__(self, benchling_service: BenchlingService, igblast_service: IgBlastService):
        self.benchling_service = benchling_service
        self.igblast_service = igblast_service
        self.mapper = IgBlastToBenchlingMapper(self.benchling_service.config)

    def execute(self, events: List[EntityRegisteredEvent]) -> SequenceAnnotationBulkFlowResults:
        with BenchlingAppSession(self.benchling_service, f'Sequence Annotation Bulk Flow ({len(events)} sequences)') as session:
            # Define steps
            steps = [
                ValidateEventsStep(self.benchling_service.config),
                GetDnaSequencesStep(self.benchling_service),
                GetSpeciesStep(self.benchling_service),
                ExecuteIgBlastStep(self.igblast_service),
                ExcludeStopCodonsStep(self.benchling_service, session, self.mapper),
                CreateAaSequencesStep(self.benchling_service, self.mapper),
                FillTranslationsStep(self.benchling_service, self.mapper),
                CreateGermlineAlignmentSequencesStep(self.benchling_service, self.mapper),
                CreateNucleotideTemplateAlignmentStep(self.benchling_service)
            ]

            # Execute steps sequentially
            step_results = []
            next_step_items = events

            for step in steps:
                session.add_message(f'{step.__label__} ({len(next_step_items)})')
                results = step.execute(next_step_items)

                step_results.append(results)
                next_step_items = results.succeeded

                if len(next_step_items) == 0:
                    break

            # Merge results
            flow_results = SequenceAnnotationBulkFlowResults.merge(step_results)

            # Report errors
            self._report_errors(session, flow_results.warning, flow_results.failed)

            # Set session and flow status
            session.session_status, flow_status = self._get_flow_status(flow_results)

            # Notify completed
            session.add_message(
                f'Flow completed {len(flow_results.succeeded)} succeeded / {len(flow_results.warning)} warnings / {len(flow_results.failed)} failed',
                flow_status
            )

            return flow_results

    def _report_errors(self, session: BenchlingAppSession, warning: List[StepFailedResult], failed: List[StepFailedResult]):
        if len(warning) > 0:
            error_message = ', '.join(set([error.error_message for error in warning]))

            session.add_message(f'{len(warning)} warning(s) detected: {error_message}', AppSessionMessageStyle.ERROR)
            logger.error(f'{len(warning)} warning(s) detected', extra={'errors': [error.to_dict() for error in warning]})

        if len(failed) > 0:
            error_message = ', '.join(set([error.error_message for error in failed]))

            session.add_message(f'{len(failed)} error(s) detected: {error_message}', AppSessionMessageStyle.ERROR)
            logger.error(f'{len(failed)} error(s) detected', extra={'errors': [error.to_dict() for error in failed]})

    def _get_flow_status(self, results: SequenceAnnotationBulkFlowResults) -> Tuple[AppSessionUpdateStatus, AppSessionMessageStyle]:
        if len(results.failed) > 0:
            if len(results.succeeded) == 0:
                return AppSessionUpdateStatus.FAILED, AppSessionMessageStyle.ERROR
            else:
                return AppSessionUpdateStatus.COMPLETED_WITH_WARNINGS, AppSessionMessageStyle.WARNING
        elif len(results.warning) > 0:
            return AppSessionUpdateStatus.COMPLETED_WITH_WARNINGS, AppSessionMessageStyle.WARNING
        else:
            return AppSessionUpdateStatus.SUCCEEDED, AppSessionMessageStyle.SUCCESS
