from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import Generic, TypeVar, List, Optional, Any, Dict

RESULT = TypeVar('RESULT')
FAILURE = TypeVar('FAILURE')


@dataclass
class StepFailedResult(Generic[RESULT], ABC):
    payload: Optional[RESULT]
    error_message: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            'payload': self.payload.to_dict(),
            'error_message': self.error_message
        }

    def __str__(self) -> str:
        return self.error_message


@dataclass
class BaseStepResults(Generic[RESULT, FAILURE], ABC):
    succeeded: List[RESULT] = field(default_factory=lambda: [])
    warning: List[StepFailedResult[FAILURE]] = field(default_factory=lambda: [])
    failed: List[StepFailedResult[FAILURE]] = field(default_factory=lambda: [])

    def add_succeeded(self, result: RESULT):
        self.succeeded.append(result)

    def add_warning(self, previous: FAILURE, warning_message: str):
        self.warning.append(StepFailedResult(previous, warning_message))

    def add_failed(self, previous: FAILURE, error_message: str):
        self.failed.append(StepFailedResult(previous, error_message))


@dataclass
class BaseRetryableStepResults(BaseStepResults[RESULT, FAILURE], ABC):
    to_retry: List[RESULT] = field(default_factory=lambda: [])


INPUT = TypeVar('INPUT')
OUTPUT = TypeVar('OUTPUT')


class BaseStep(Generic[INPUT, OUTPUT], ABC):
    __label__ = None

    @abstractmethod
    def execute(self, arg: INPUT) -> OUTPUT:
        pass


class BaseFlow(BaseStep[INPUT, OUTPUT], ABC):
    pass
