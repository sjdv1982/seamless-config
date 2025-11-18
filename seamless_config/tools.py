from copy import deepcopy
from typing import Any
import re

_tools: dict[str, dict] = {}

DOLLAR_RE = re.compile(r"\$[A-Za-z_][A-Za-z0-9_]*")


def define_tools(tools: dict[str, dict]):
    for toolname, tool in tools.items():
        try:
            ADDED = tool["ADDED"]
            assert isinstance(ADDED, list)
            for v in ADDED:
                assert isinstance(v, str)

            INJECTED = tool["INJECTED"]
            assert isinstance(INJECTED, list)
            for v in INJECTED:
                assert isinstance(v, str)

        except Exception as exc:
            raise ValueError(f"{toolname}: {type(exc).__name__}: {exc}")

        _tools[toolname] = deepcopy(tool)


def _configure_tool(tool: str, *, added: dict[str, Any], injected: dict[str, Any]):
    conf = _tools[tool]
    ADDED, INJECTED = conf["ADDED"], conf["INJECTED"]
    for k in ADDED:
        if k not in added:
            raise ValueError(f'"{k}" must be added')
    for k in INJECTED:
        if k not in injected:
            raise ValueError(f'"{k}" must be injected')

    result = deepcopy(added)
    for key0, value0 in conf.items():
        if key0 in ("ADDED", "INJECTED"):
            continue
        if key0.endswith("_template"):
            key = key0[: -len("_template")]
            value = value0
            for m in reversed(list(DOLLAR_RE.finditer(value0))):
                inj = injected[m.group()[1:]]
                start, end = m.span()
                value = value[:start] + str(inj) + value[end:]
        else:
            key = key0
            value = value0
        result[key] = value

    # Special case: hostname
    # => remove if CLUSTER is equal to get_local_cluster
    cluster = injected.get("CLUSTER")
    if cluster == get_local_cluster():
        result.pop("hostname")

    return result


def _build_injected(mode: str, cluster: str, project, subproject, stage, substage):
    assert isinstance(mode, str) and mode in ("ro", "rw"), mode
    injected = {"CLUSTER": cluster, "MODE": mode}
    projectsubdir = "/" + project
    if subproject is not None:
        projectsubdir += "/" + subproject
    injected["PROJECTSUBDIR"] = projectsubdir
    stagesubdir = ""
    if stage is not None:
        stagesubdir = "/STAGE-" + stage
        if substage is not None:
            stagesubdir += "SUBSTAGE-" + substage
    injected["STAGESUBDIR"] = stagesubdir
    return injected


from .cluster import get_cluster, get_local_cluster, ClusterFrontend


def _prepare_tool(
    tool: str,
    mode: str,
    cluster,
    project,
    subproject,
    stage,
    substage,
    frontend_name,
) -> tuple[ClusterFrontend, dict[str, str]]:
    from .select import get_current
    from . import ConfigurationError

    cluster, project, subproject, stage, substage = get_current(
        cluster, project, subproject, stage, substage
    )
    clus = get_cluster(cluster)
    for frontend in clus.frontends:
        if frontend_name is not None and frontend.hostname != frontend_name:
            continue
        if getattr(frontend, tool) is not None:
            break
    else:
        if frontend_name is not None:
            raise ConfigurationError(
                f"No frontend of cluster '{cluster}' with name '{frontend_name}' can support a {tool}"
            )
        else:
            raise ConfigurationError(
                f"No frontend of cluster '{cluster}' can support a {tool}"
            )
    injected = _build_injected(mode, cluster, project, subproject, stage, substage)
    return clus, frontend, injected


def configure_hashserver(
    mode: str,
    *,
    cluster=None,
    project=None,
    subproject=None,
    stage=None,
    substage=None,
    frontend_name=None,
):

    clus, frontend, injected = _prepare_tool(
        "hashserver", mode, cluster, project, subproject, stage, substage, frontend_name
    )
    assert frontend.hashserver is not None
    injected["BUFFERDIR"] = frontend.hashserver.bufferdir

    added = {}
    added["tunnel"] = clus.tunnel
    added["hostname"] = frontend.hostname
    if frontend.ssh_hostname is not None:
        added["ssh_hostname"] = frontend.ssh_hostname
    added["network_interface"] = frontend.hashserver.network_interface
    added["conda"] = frontend.hashserver.conda
    added["port_start"] = frontend.hashserver.port_start
    added["port_end"] = frontend.hashserver.port_end

    return _configure_tool("hashserver", added=added, injected=injected)


def configure_database(
    mode: str,
    *,
    cluster=None,
    project=None,
    subproject=None,
    stage=None,
    substage=None,
    frontend_name=None,
):

    clus, frontend, injected = _prepare_tool(
        "database", mode, cluster, project, subproject, stage, substage, frontend_name
    )
    assert frontend.hashserver is not None
    injected["DATABASE_DIR"] = frontend.database.database_dir

    added = {}
    added["tunnel"] = clus.tunnel
    added["hostname"] = frontend.hostname
    if frontend.ssh_hostname is not None:
        added["ssh_hostname"] = frontend.ssh_hostname
    added["network_interface"] = frontend.database.network_interface
    added["conda"] = frontend.database.conda
    added["port_start"] = frontend.database.port_start
    added["port_end"] = frontend.database.port_end

    return _configure_tool("database", added=added, injected=injected)
