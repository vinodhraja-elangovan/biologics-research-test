import itertools
from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import List, Dict, Generic, TypeVar

from benchling_api_client.v2.extensions import NotPresentError
from benchling_sdk.models import AsyncTask

from sequence_annotation.flows.base import RESULT, FAILURE, OUTPUT, INPUT, BaseStep, BaseStepResults, BaseRetryableStepResults

STEP_RESULT = TypeVar('STEP_RESULT', bound=BaseStepResults)


@dataclass
class AsyncTaskResults(Generic[RESULT, FAILURE], BaseRetryableStepResults[RESULT, FAILURE]):
    pass


class BaseAsyncTaskStep(Generic[INPUT, STEP_RESULT], BaseStep[List[INPUT], STEP_RESULT], ABC):
    @abstractmethod
    def initialize_results(self) -> STEP_RESULT:
        pass

    @abstractmethod
    def get_task(self, items: List[INPUT]) -> AsyncTask:
        pass

    @abstractmethod
    def get_succeeded_items(self, items: List[INPUT], task: AsyncTask) -> List[OUTPUT]:
        pass

    def execute(self, items: List[INPUT]) -> STEP_RESULT:
        results = self.initialize_results()

        if len(items) == 0:
            return results

        task = self.get_task(items)

        task_results = AsyncTaskResultProcessor(task).process(items)

        if len(task_results.failed) > 0:
            results.failed.extend(task_results.failed)

            retry_results = self.execute(task_results.to_retry)

            results.succeeded.extend(retry_results.succeeded)
            results.failed.extend(retry_results.failed)
        else:
            for succeeded_item in self.get_succeeded_items(items, task):
                results.add_succeeded(succeeded_item)

        return results


class AsyncTaskResultProcessor(Generic[RESULT, FAILURE]):
    def __init__(self, task: AsyncTask):
        self.task = task

    def _has_async_task_errors(self) -> bool:
        try:
            return self.task.errors and len(self.task.errors.additional_properties) > 0
        except NotPresentError:
            return False

    def _get_error_message_by_index(self) -> Dict[int | str, str]:
        if isinstance(self.task.errors.additional_properties, list):
            return {
                index: '\n'.join([error.get('message') for error in list(values) if 'message' in error])
                for index, values in itertools.groupby(self.task.errors, lambda e: e.get('index'))
            }
        elif isinstance(self.task.errors.additional_properties, dict):
            return self.task.errors.additional_properties
        else:
            raise Exception('Unable to parse errors from AsyncTask')

    def process(self, inputs: List[RESULT]) -> AsyncTaskResults[RESULT, FAILURE]:
        results = AsyncTaskResults()

        # If any errors found, the whole payload has been rollback
        if self._has_async_task_errors():
            error_message_by_index = self._get_error_message_by_index()

            # If index == message -> add all items to failed results with task message
            if error_message_by_index.get('index') == 'message':
                # Add inputs to failed results
                for item in inputs:
                    results.add_failed(item, self.task.message)
            else:
                # Add failed sequences to results
                for index, error_message in error_message_by_index.items():
                    results.add_failed(inputs[index], error_message)

                # Remove failed sequences from inputs
                inputs = [item for i, item in enumerate(inputs) if i not in error_message_by_index.keys()]

                # Add succeeded sequences to retry list
                for item in inputs:
                    results.to_retry.append(item)
        else:
            for item in inputs:
                results.add_succeeded(item)

        return results
