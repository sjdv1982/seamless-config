from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

import yaml  # type: ignore

from . import get_workdir
from .cluster import define_clusters as register_clusters
from .select import (
    get_stage,
    reset_execution_before_load,
    select_cluster,
    select_execution,
    select_project,
    select_subproject,
)
from .tools import define_tools

TOOLS_FILENAME = "tools.yaml"
CONFIG_FILENAMES: tuple[str, str] = ("seamless.yaml", ".seamless.yaml")
INHERIT_COMMAND = "inherit_from_parent"
COMMAND_LIST_EXAMPLE = "- project: myproject"

_clusters: dict[str, Any] = {}


@dataclass(frozen=True)
class CommandSpec:
    handler: Callable[[Any, Path], None]
    priority: bool = False


@dataclass
class CommandInvocation:
    name: str
    argument: Any
    spec: CommandSpec
    source: Path

    @property
    def priority(self) -> bool:
        return self.spec.priority

    def execute(self) -> None:
        self.spec.handler(self.argument, self.source)


@dataclass(frozen=True)
class StageBlock:
    stage: str
    entries: list[Any]


_tools_loaded = False


# Tool definition
def load_tools() -> dict:
    """
    Load tool definitions from tools.yaml and register them inside seamless_config.tools.
    """
    global _tools_loaded
    if _tools_loaded:
        return
    _tools_loaded = True
    tools_file = Path(__file__).with_name(TOOLS_FILENAME)
    with tools_file.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if data is None:
        data = {}
    if not isinstance(data, dict):
        raise ValueError(
            f"Expected a mapping in {tools_file}, found {type(data).__name__}"
        )

    define_tools(data)
    return data


# Command language
def _handle_cluster(value: Any, source: Path) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{source}: 'cluster' command expects a string value")
    select_cluster(value)


def _handle_execution(value: Any, source: Path) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{source}: 'execution' command expects a string value")
    select_execution(value, source="command")


def _handle_project(value: Any, source: Path) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{source}: 'project' command expects a string value")
    select_project(value)


def _handle_subproject(value: Any, source: Path) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{source}: 'subproject' command expects a string value")
    select_subproject(value)


def _handle_clusters(value: Any, source: Path) -> None:
    if not isinstance(value, dict):
        raise ValueError(f"{source}: 'clusters' command expects a mapping")
    _clusters.update(value)


COMMAND_SPECS: dict[str, CommandSpec] = {
    "cluster": CommandSpec(handler=_handle_cluster),
    "execution": CommandSpec(handler=_handle_execution),
    "project": CommandSpec(handler=_handle_project),
    "subproject": CommandSpec(handler=_handle_subproject),
    "clusters": CommandSpec(handler=_handle_clusters, priority=True),
}


def _load_clusters() -> dict[str, Any]:
    """
    Load cluster definitions from $HOME/.seamless/clusters.yaml into _clusters.
    """
    global _clusters
    home_dir = os.environ.get("HOME") or str(Path.home())
    clusters_path = Path(home_dir) / ".seamless" / "clusters.yaml"
    data: Any = {}
    if clusters_path.is_file():
        with clusters_path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    if data is None:
        data = {}
    if not isinstance(data, dict):
        raise ValueError(
            f"{clusters_path}: expected a mapping with cluster definitions"
        )
    _clusters = data
    return _clusters


# File location and parsing
def load_config_files() -> None:
    """
    Load Seamless configuration files and execute their commands.
    """
    reset_execution_before_load()
    load_tools()
    _load_clusters()
    commands = _build_command_invocations(_collect_command_entries())
    priority_commands = [cmd for cmd in commands if cmd.priority]
    non_priority_commands = [cmd for cmd in commands if not cmd.priority]

    for command in priority_commands:
        command.execute()

    register_clusters(_clusters)

    for command in non_priority_commands:
        command.execute()


def _collect_command_entries() -> list[tuple[Path, Any]]:
    commands_by_directory: list[list[tuple[Path, Any]]] = []
    current_dir = Path(get_workdir()).resolve()
    while True:
        directory_entries: list[tuple[Path, Any]] = []
        inherit = False
        for filename in CONFIG_FILENAMES:
            yaml_path = current_dir / filename
            if not yaml_path.is_file():
                continue
            entries = _read_yaml_list(yaml_path)
            directory_entries.extend((yaml_path, entry) for entry in entries)
            if _list_contains_inherit(entries):
                inherit = True
        commands_by_directory.append(directory_entries)
        if not inherit:
            break
        parent = current_dir.parent
        if parent == current_dir:
            break
        current_dir = parent
    commands: list[tuple[Path, Any]] = []
    for directory_entries in reversed(commands_by_directory):
        commands.extend(directory_entries)
    return commands


def _read_yaml_list(path: Path) -> list[Any]:
    with path.open("r", encoding="utf-8") as handle:
        content = yaml.safe_load(handle)
    if not isinstance(content, list):
        raise ValueError(
            f"{path}: expected a YAML list of commands. Example:\n{COMMAND_LIST_EXAMPLE}"
        )
    return content


def _list_contains_inherit(entries: Sequence[Any]) -> bool:
    return any(_extract_command_name(entry) == INHERIT_COMMAND for entry in entries)


def _build_command_invocations(
    entries: Iterable[tuple[Path, Any]],
) -> list[CommandInvocation]:
    commands: list[CommandInvocation] = []
    for path, entry in entries:
        name, argument = _parse_command_entry(entry, path)
        if name == INHERIT_COMMAND:
            continue
        if name == "stage":
            commands.extend(_expand_stage_commands(argument, path))
            continue
        spec = COMMAND_SPECS.get(name)
        if spec is None:
            raise ValueError(f"{path}: unknown command '{name}'")
        commands.append(
            CommandInvocation(name=name, argument=argument, spec=spec, source=path)
        )
    return commands


def _parse_command_entry(entry: Any, source: Path) -> tuple[str, Any]:
    if isinstance(entry, str):
        return entry, None
    if isinstance(entry, dict) and len(entry) == 1:
        key = next(iter(entry))
        if not isinstance(key, str):
            raise ValueError(f"{source}: command names must be strings")
        value = entry[key]
        stage_block = _maybe_build_stage_block(key, value, source)
        if stage_block is not None:
            return "stage", stage_block
        return key, value
    raise ValueError(
        f"{source}: commands must be defined as strings or single-key mappings"
    )


def _maybe_build_stage_block(key: str, value: Any, source: Path) -> StageBlock | None:
    prefix = "stage"
    if not key.startswith(prefix):
        return None
    remainder = key[len(prefix) :].strip()
    if not remainder:
        return None
    stage_name = remainder
    if not isinstance(value, list):
        raise ValueError(
            f"{source}: 'stage {stage_name}' must be followed by a YAML list of commands"
        )
    return StageBlock(stage=stage_name, entries=list(value))


def _expand_stage_commands(block: StageBlock, source: Path) -> list[CommandInvocation]:
    current_stage = get_stage()
    if current_stage != block.stage:
        return []
    nested_entries = [(source, entry) for entry in block.entries]
    return _build_command_invocations(nested_entries)


def _extract_command_name(entry: Any) -> str | None:
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict) and len(entry) == 1:
        key = next(iter(entry))
        if isinstance(key, str):
            return key
    return None
