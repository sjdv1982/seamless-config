from typing import Optional

_current_cluster: Optional[str] = None
_current_project: Optional[str] = None
_current_subproject: Optional[str] = None
_current_stage: Optional[str] = None
_current_substage: Optional[str] = None
_current_execution: str = "process"
_execution_source: Optional[str] = None  # "command" or "manual"
_execution_command_seen: bool = False

EXECUTION_MODES = ("process", "spawn", "remote")

from . import ConfigurationError


def _validate(s, name):
    if s.find("--") > -1:
        raise ValueError(f"{name} cannot contain '--'")
    if s.find("STAGE-") > -1:
        raise ValueError(f"{name} cannot contain 'STAGE-'")
    if name != "subproject":
        if s.find("/") > -1:
            raise ValueError(f"{name} cannot contain '/'")


def select_cluster(cluster):
    global _current_cluster
    if cluster is not None:
        _validate(cluster, "cluster")
    _current_cluster = cluster


def select_project(project):
    global _current_project
    _validate(project, "project")
    _current_project = project


def select_subproject(subproject):
    global _current_subproject
    if subproject is not None:
        _validate(subproject, "subproject")
    _current_subproject = subproject


def select_stage(stage):
    global _current_stage
    if stage is not None:
        _validate(stage, "stage")
    _current_stage = stage


def select_substage(substage):
    global _current_substage
    if substage is not None:
        _validate(substage, "substage")
    _current_substage = substage


def select_execution(execution: str, *, source: str = "manual") -> None:
    global _current_execution, _execution_source, _execution_command_seen
    if not isinstance(execution, str):
        raise ValueError("execution must be a string")
    if execution not in EXECUTION_MODES:
        valid = ", ".join(EXECUTION_MODES)
        raise ValueError(f"execution must be one of: {valid}")
    _current_execution = execution
    _execution_source = source
    if source == "command":
        _execution_command_seen = True


def get_stage():
    return _current_stage


def get_execution() -> str:
    return _current_execution


def execution_was_set_explicitly() -> bool:
    return _execution_source is not None


def execution_command_seen() -> bool:
    return _execution_command_seen


def reset_execution_before_load() -> None:
    global _execution_source, _execution_command_seen, _current_execution
    _execution_command_seen = False
    if _execution_source == "command":
        _execution_source = None
        _current_execution = "process"


def get_selected_cluster() -> Optional[str]:
    return _current_cluster


def get_current(
    cluster: Optional[str] = None,
    project: Optional[str] = None,
    subproject: Optional[str] = None,
    stage: Optional[str] = None,
    substage: Optional[str] = None,
):
    if cluster is None:
        cluster = _current_cluster
        if cluster is None:
            raise ConfigurationError("No cluster defined")

    if project is None:
        project = _current_project
        if project is None:
            raise ConfigurationError("No project defined")

    if subproject is None:
        subproject = _current_subproject

    if stage is None:
        stage = _current_stage

    if substage is None:
        substage = _current_substage

    return cluster, project, subproject, stage, substage
