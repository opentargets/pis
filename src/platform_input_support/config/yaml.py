import sys
from pathlib import Path
from typing import Any

import yaml
from loguru import logger
from pydantic import ValidationError

from platform_input_support.config.models import BaseTaskDefinition, YamlSettings


def load_yaml_file(config_file: Path) -> str:
    logger.debug(f'loading yaml file {config_file}')
    try:
        return Path.read_text(config_file)
    except OSError as e:
        logger.critical(f'error reading config file: {e}')
        sys.exit(1)


def parse_yaml_string(yaml_string: str) -> dict:
    logger.debug('parsing yaml string')
    try:
        return yaml.safe_load(yaml_string) or {}
    except yaml.YAMLError as e:
        logger.critical(f'error parsing config file: {e}')
        sys.exit(1)


def parse_yaml(config_file: Path) -> dict[str, Any]:
    """Parse a yaml file.

    This function loads a yaml file, parses its content, and returns it as a
    dictionary. If the file cannot be read or the content cannot be parsed, the
    program will log an error and exit.

    Args:
        config_file (Path): The path to the yaml file.

    Returns:
        dict: The parsed yaml content.
    """
    yaml_str = load_yaml_file(config_file)
    return parse_yaml_string(yaml_str)


def get_yaml_settings(yaml_dict: dict[str, Any]) -> YamlSettings:
    """Validate the yaml settings.

    This function validates the yaml settings against the YamlSettings model. If
    the settings are invalid, the program will log an error and exit.

    Args:
        yaml_dict (dict[str, Any]): The yaml settings.

    Returns:
        YamlSettings: The validated yaml settings.
    """
    try:
        return YamlSettings.model_validate(yaml_dict)
    except ValidationError as e:
        logger.critical(f'error validating yaml settings: {e}')
        sys.exit(1)


def get_yaml_stepdefs(yaml_dict: dict[str, Any]) -> dict[str, list[BaseTaskDefinition]]:
    """Validate the yaml step definitions.

    This function validates the yaml step definitions against the BaseTaskDefinition
    model. If the step definitions are invalid, the program will log an error and
    exit.

    Args:
        yaml_dict (dict[str, Any]): The yaml step definitions.

    Returns:
        dict[str, list[BaseTaskDefinition]]: The validated yaml step definitions.
    """
    steps = yaml_dict.get('steps', {})

    # this part must be validated separately, as this dict does not have a model
    if not isinstance(steps, dict):
        logger.critical('steps must be a dictionary')
        sys.exit(1)
    try:
        return {k: [BaseTaskDefinition.model_validate(td) for td in v] for k, v in steps.items()}
    except ValidationError as e:
        logger.critical(f'error validating yaml stepdefs: {e}')
        sys.exit(1)


def get_yaml_sentinel_dict(yaml_dict: dict[str, Any]) -> dict[str, Any]:
    """Get the yaml sentinel dictionary.

    This function returns the sentinel dictionary for a scratchpad from the yaml
    settings. If the sentinel dictionary is not present, an empty dictionary is
    returned.

    Args:
        yaml_dict (dict[str, Any]): The yaml settings.

    Returns:
        dict[str, Any]: The sentinel dictionary.
    """
    return yaml_dict.get('scratchpad', {})
