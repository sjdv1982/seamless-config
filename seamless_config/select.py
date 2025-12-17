from typing import Optional

_current_cluster: Optional[str] = None
_current_project: Optional[str] = None
_current_subproject: Optional[str] = None
_current_stage: Optional[str] = None
_current_substage: Optional[str] = None
_current_execution: str = "process"
_current_queue: Optional[str] = None
_current_remote: Optional[str] = None
_execution_source: Optional[str] = None  # "command" or "manual"
_queue_source: Optional[str] = None  # "command" or "manual"
_queue_cluster: Optional[str] = None
_remote_source: Optional[str] = None  # "command" or "manual"
_execution_command_seen: bool = False

EXECUTION_MODES = ("process", "spawn", "remote")
REMOTE_TARGETS = (None, "daskserver", "jobserver")

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
    global _current_cluster, _current_queue, _queue_source, _queue_cluster
    if cluster is not None:
        _validate(cluster, "cluster")
    if cluster != _current_cluster:
        _current_queue = None
        _queue_source = None
        _queue_cluster = None
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


def select_queue(queue: str, *, source: str = "manual") -> None:
    global _current_queue, _queue_source, _queue_cluster
    if not isinstance(queue, str):
        raise ValueError("queue must be a string")
    cluster = _current_cluster
    if cluster is None:
        raise ValueError("Cannot select a queue without selecting a cluster first")
    from .cluster import get_cluster

    try:
        clus = get_cluster(cluster)
    except KeyError:
        raise ValueError(f"Unknown cluster '{cluster}'") from None
    queues = clus.queues
    if not queues:
        raise ValueError(f"Cluster '{cluster}' has no queues")
    if queue not in queues:
        raise ValueError(f"Cluster '{cluster}' has no queue '{queue}'")
    _current_queue = queue
    _queue_source = source
    _queue_cluster = cluster


def select_remote(remote: Optional[str], *, source: str = "manual") -> None:
    global _current_remote, _remote_source
    if remote is not None and not isinstance(remote, str):
        raise ValueError("remote must be a string or null")
    if remote not in REMOTE_TARGETS:
        valid = ", ".join([target for target in REMOTE_TARGETS if target is not None])
        raise ValueError(f"remote must be one of: None, {valid}")
    _current_remote = remote
    _remote_source = source


def get_stage():
    return _current_stage


def get_execution() -> str:
    return _current_execution


def execution_was_set_explicitly() -> bool:
    return _execution_source is not None


def execution_command_seen() -> bool:
    return _execution_command_seen


def get_queue(cluster: Optional[str] = None) -> Optional[str]:
    if cluster is None:
        cluster = _current_cluster
    if cluster is None:
        return None
    if _current_queue is None:
        return None
    if _queue_cluster != cluster:
        return None
    from .cluster import get_cluster

    try:
        clus = get_cluster(cluster)
    except KeyError:
        return None
    queues = clus.queues
    if not queues or _current_queue not in queues:
        return None
    return _current_queue


def get_remote() -> Optional[str]:
    return _current_remote


def reset_execution_before_load() -> None:
    global _execution_source, _execution_command_seen, _current_execution
    _execution_command_seen = False
    if _execution_source == "command":
        _execution_source = None
        _current_execution = "process"


def reset_queue_before_load() -> None:
    global _current_queue, _queue_source, _queue_cluster
    if _queue_source == "command":
        _current_queue = None
        _queue_cluster = None
        _queue_source = None


def reset_remote_before_load() -> None:
    global _current_remote, _remote_source
    if _remote_source == "command":
        _current_remote = None
        _remote_source = None


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
