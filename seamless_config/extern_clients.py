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
        database_entries.append({"readonly": info["readonly"], "url": info.get("url")})

    for info in database_remote.inspect_launched_clients():
        if info.get("cluster") != cluster:
            continue
        database_entries.append({"readonly": info["readonly"], "url": info.get("url")})

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


def set_remote_clients(clients: Dict[str, List[Dict[str, Any]]]) -> None:
    """
    Configure extern buffer/database clients from a collected definition.
    """
    from seamless_remote import buffer_remote, database_remote
    import seamless_config as _config

    if _config._initialized:
        raise RuntimeError("Cannot set remote clients after initialization")
    _config._remote_clients_set = True

    database = clients.get("database", [])
    database_names = []
    buffer = clients.get("buffer", [])
    buffer_names = []

    for idx, entry in enumerate(database):
        readonly = entry.get("readonly", True)
        url = entry.get("url")
        if url is None:
            raise ValueError("Database client entry requires 'url'")
        name = f"extern-db-{idx}"
        database_remote.define_extern_client(
            name, "database", url=url, readonly=readonly
        )
        database_names.append(name)

    database_remote.activate(no_main=True, extern_clients=database_names)

    buffer_names = []
    for idx, entry in enumerate(buffer):
        readonly = entry.get("readonly", True)
        url = entry.get("url")
        directory = entry.get("directory")
        name = f"extern-buffer-{idx}"
        if directory is not None and url is None:
            buffer_remote.define_extern_client(
                name, "bufferfolder", directory=directory, readonly=True
            )
        elif url is not None:
            buffer_remote.define_extern_client(
                name, "hashserver", url=url, readonly=readonly
            )
        else:
            raise ValueError("Buffer client entry requires 'url' or 'directory'")
        buffer_names.append(name)

    buffer_remote.activate(no_main=True, extern_clients=buffer_names)
