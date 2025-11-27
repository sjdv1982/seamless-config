from __future__ import annotations

from typing import Any, Dict, List

from seamless_remote import buffer_remote, database_remote

from .tools import configure_hashserver


def collect_remote_clients(cluster: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Collect extern and launched database/buffer clients for a given cluster.

    Returns two lists with entries that can be passed to define_extern_client,
    under the keys "database" and "buffer".
    """
    database_entries: list[dict[str, Any]] = []
    buffer_entries: list[dict[str, Any]] = []

    for info in database_remote.inspect_extern_clients():
        database_entries.append(
            {"readonly": info["readonly"], "url": info.get("url")}
        )

    for info in database_remote.inspect_launched_clients():
        if info.get("cluster") != cluster:
            continue
        database_entries.append(
            {"readonly": info["readonly"], "url": info.get("url")}
        )

    for info in buffer_remote.inspect_extern_clients():
        entry: dict[str, Any] = {"readonly": info["readonly"]}
        if info.get("directory") is not None:
            entry["directory"] = info["directory"]
        if info.get("url") is not None:
            entry["url"] = info["url"]
        buffer_entries.append(entry)

    for info in buffer_remote.inspect_launched_clients():
        if info.get("cluster") != cluster:
            continue
        readonly = bool(info["readonly"])
        entry: dict[str, Any] = {"readonly": readonly}
        if info.get("directory") is not None:
            entry["directory"] = info["directory"]
        if info.get("url") is not None:
            entry["url"] = info["url"]

        # For readonly clients we replace the URL entry with a directory entry.
        if not readonly:
            buffer_entries.append(entry)

        mode = "ro" if readonly else "rw"
        workdir_conf = configure_hashserver(
            mode,
            cluster=info.get("cluster"),
            project=info.get("project"),
            subproject=info.get("subproject"),
            stage=info.get("stage"),
        )
        workdir = workdir_conf.get("workdir")
        buffer_entries.append({"readonly": readonly, "directory": workdir})

    return {"database": database_entries, "buffer": buffer_entries}
