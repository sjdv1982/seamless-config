"""Launch a Dask server without Seamless dependencies (pure Dask mode)."""

from __future__ import annotations

from typing import Any

from . import ConfigurationError

DISABLED = False  # to disable automatic activation during tests

_launcher_cache: dict[Any, dict] = {}
_launched_handle: "PureDaskserverLaunchedHandle | None" = None


def _freeze_value(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple(sorted((k, _freeze_value(v)) for k, v in value.items()))
    if isinstance(value, list):
        return tuple(_freeze_value(v) for v in value)
    return value


class PureDaskserverLaunchedHandle:
    """Synchronous launcher that yields a distributed.Client."""

    launch_config: dict
    launch_payload: dict
    client: Any
    dashboard_url: str | None

    def __init__(self, cluster: str, queue: str | None, frontend_name: str | None):
        self.dashboard_url = None
        self.config(cluster, queue, frontend_name)
        self._do_init()

    def config(
        self, cluster: str, queue: str | None, frontend_name: str | None
    ) -> None:
        from .tools import configure_pure_daskserver

        self.launch_config = configure_pure_daskserver(
            cluster=cluster, queue=queue, frontend_name=frontend_name
        )

    def _do_init(self) -> None:
        import remote_http_launcher

        conf = self.launch_config
        frozenconf = _freeze_value(conf)
        payload = _launcher_cache.get(frozenconf)
        if payload is None:
            print("Launch daskserver...")
            payload = remote_http_launcher.run(conf)
            _launcher_cache[frozenconf] = payload

        self.launch_payload = payload
        hostname = payload.get("hostname", "localhost")
        scheduler_port = int(payload["port"])
        dashboard_port = payload.get("dashboard_port")
        if dashboard_port is not None:
            try:
                dashboard_port = int(dashboard_port)
                self.dashboard_url = f"http://{hostname}:{dashboard_port}"
            except Exception:
                self.dashboard_url = None

        scheduler_address = f"tcp://{hostname}:{scheduler_port}"

        from distributed import Client as DistributedClient

        self.client = DistributedClient(
            scheduler_address, timeout="10s", set_as_default=False
        )


def activate(*, no_main: bool = False, queue: str | None = None) -> None:
    """Launch the remote daskserver and configure a distributed.Client."""

    global _launched_handle
    if DISABLED or no_main:
        return
    from .select import get_selected_cluster

    cluster = get_selected_cluster()
    if cluster is None:
        raise ConfigurationError("No cluster defined")

    _launched_handle = PureDaskserverLaunchedHandle(cluster, queue, None)
    if _launched_handle.dashboard_url:
        print(f"Dask dashboard: {_launched_handle.dashboard_url}")


def deactivate() -> None:
    """Clear the current Dask client and launched handle."""

    global _launched_handle
    if _launched_handle is None:
        return
    try:
        client = getattr(_launched_handle, "client", None)
        if client is not None:
            client.close(timeout="2s")
    except Exception:
        pass
    _launched_handle = None


def get_client():
    """Return the current distributed.Client if pure Dask mode is active."""

    if _launched_handle is None:
        return None
    return _launched_handle.client


__all__ = ["activate", "deactivate", "get_client", "PureDaskserverLaunchedHandle"]
