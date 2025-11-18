from typing import Optional

_current_cluster: Optional[str] = None
_current_project: Optional[str] = None
_current_subproject: Optional[str] = None
_current_stage: Optional[str] = None
_current_substage: Optional[str] = None

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


def get_stage():
    return _current_stage


def get_current(cluster=None, project=None, subproject=None, stage=None, substage=None):
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
