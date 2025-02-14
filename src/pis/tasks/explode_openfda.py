"""Generate tasks for openfda file download."""

import json
from pathlib import Path
from typing import Any, Self

import jq
from loguru import logger
from otter.scratchpad.model import Scratchpad
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report


class ExplodeOpenfdaSpec(Spec):
    """Configuration fields for the explode_openfda task.

    This task has the following custom configuration fields:
        - do (list[dict]): The tasks to explode. Each task in the list will be
            duplicated for each iteration of the foreach list.
        - json_path (Path): The path to the json file to iterate over. This is a
            local path. Use this task in conjunction with the :py:mod:`otter.tasks.copy`
            task to download the json file.
        - jq_filter (str): A jq filter to apply to the json file. This filter should
            return a list of strings.
        - prefix (str): This string will be removed from the beginning of each string
            in the list to generate the destination path.
    """

    do: list[Spec]
    json_path: Path
    jq_filter: str
    prefix: str

    def model_post_init(self, __context: Any) -> None:
        # allows keys to be missing from the global scratchpad
        self.scratchpad_ignore_missing = True


class ExplodeOpenfda(Task):
    """Generate tasks for openfda file download.

    This task will duplicate the specs in the `do` list for each entry in a list
    that will be obtained from a json file specified by the local path in `json_path`.
    The `jq_filter` field will be used to extract the list of strings from the json
    file.

    .. note:: The `jq` command must be installed in the system to use this task.
    .. note:: Use this task in conjunction with the :py:mod:`otter.tasks.copy` task
        to download the json file.
    .. note:: The `prefix` field is used to remove a common prefix from the strings
        in the list before using them in the new tasks, so the string e.g.:

        ```
            https://download.open.fda.gov/drug/event/2019q4/drug-event-0028-of-0029.json.zip
        ```
        can become:
        ```
            2019q4/drug-event-0028-of-0029.json.zip
        ```
        for the path generation to work.


    Inside of the specs in the `do` list, the string `each` can be used as as a
    sentinel to refer to the current iteration value.

    .. warning:: The `${each}` placeholder **MUST** be present in the :py:obj:`otter.task.model.Spec.name`
        of the new specs defined inside `do`, as otherwise all of them will have
        the same name, and it must be unique.

    Keep in mind this replacement of `each` will only be done in strings, not lists
    or sub-objects.
    """

    def __init__(self, spec: ExplodeOpenfdaSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: ExplodeOpenfdaSpec
        self.scratchpad = Scratchpad({
            'prefix': spec.prefix,
        })
        self.json_local_path = context.config.work_path / spec.json_path

    @report
    def run(self) -> Self:
        description = self.spec.name.split(' ', 1)[1]
        json_file_content = self.json_local_path.read_text()
        json_data = json.loads(json_file_content)
        sources = jq.compile(self.spec.jq_filter).input_value(json_data).all()
        trimmed_sources = [source.replace(self.spec.prefix, '') for source in sources]
        logger.debug(f'exploding {description} into {len(self.spec.do)} tasks by {len(trimmed_sources)} iterations')
        new_tasks = 0

        for i in trimmed_sources:
            self.scratchpad.store('each', i)

            for do_spec in self.spec.do:
                replaced_do_spec = Spec.model_validate(self.scratchpad.replace_dict(do_spec.model_dump()))
                self.context.specs.append(replaced_do_spec)
                new_tasks += 1

        logger.info(f'exploded into {new_tasks} new tasks')
        return self
