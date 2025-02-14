"""Simple hello world task."""

from typing import Self

from loguru import logger
from otter.manifest.model import Artifact
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report


class HelloWorldSpec(Spec):
    """Configuration fields for the hello_world task.

    This task has the following custom configuration fields:
        - who (str): The person to greet in the output file.
    """

    who: str = 'world'


class HelloWorld(Task):
    """Simple hello world task."""

    def __init__(self, spec: HelloWorldSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: HelloWorldSpec

    @report
    def run(self) -> Self:
        # write log
        logger.info(f'hello, {self.spec.who}!')
        # set the resource
        self.resource = Artifact(source='me', destination=self.spec.who)

        return self

    @report
    def validate(self) -> Self:
        return self
