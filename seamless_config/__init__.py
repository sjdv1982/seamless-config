import inspect
import os
import sys
import warnings
from typing import Optional


class ConfigurationError(RuntimeError):
    pass


_workdir = None
_set_workdir_called = False
_initialized = False
_remote_clients_set = False
_UNSET = object()


def get_workdir():
    """Return the configured workdir or fall back to the current directory."""
    return _workdir if _workdir is not None else os.getcwd()


def _set_workdir(workdir, nback):
    """
    Optionally set the workdir. If no argument is provided, infer it from the caller.
    """
    if _remote_clients_set:
        raise RuntimeError("remote clients already set; workdir cannot be changed")
    global _workdir, _set_workdir_called
    _set_workdir_called = True
    if workdir is not _UNSET:
        if workdir is None:
            _workdir = None
        else:
            _workdir = os.path.abspath(os.fspath(workdir))
        return

    frame = inspect.currentframe()
    for n in range(nback):
        frame = frame.f_back if frame is not None else None
    caller_frame = frame
    try:
        if caller_frame is not None:
            caller_file = caller_frame.f_globals.get("__file__")
            if caller_file:
                caller_path = os.path.abspath(
                    caller_file
                    if os.path.isdir(caller_file)
                    else os.path.dirname(caller_file)
                )
                _workdir = caller_path
                return
        _workdir = None
    finally:
        # Avoid reference cycles introduced by inspect.currentframe
        del frame
        del caller_frame


def set_workdir(workdir=_UNSET):
    """
    Optionally set the workdir. If no argument is provided, infer it from the caller.
    """
    return _set_workdir(workdir, 2)


def _report_execution_requirements():
    from .select import (
        execution_command_seen,
        execution_was_set_explicitly,
        get_execution,
        get_persistent,
        persistent_was_set_explicitly,
        get_selected_cluster,
    )

    execution = get_execution()
    persistent = get_persistent()
    cluster = get_selected_cluster()
    if not execution_command_seen() and not execution_was_set_explicitly():
        if cluster is not None:
            warnings.warn(
                "No 'execution' command found; Seamless defaults to 'remote' because a cluster is defined",
                stacklevel=0,
            )
        else:
            warnings.warn(
                "No 'execution' command found; Seamless falls back to 'process'",
                stacklevel=0,
            )
    if execution == "remote" and cluster is None:
        raise ConfigurationError("Execution is 'remote' but no cluster was defined")
    if execution != "remote" and cluster is None:
        if persistent and persistent_was_set_explicitly():
            raise ConfigurationError(
                "Persistence was explicitly enabled but no cluster was defined"
            )
        if not persistent and persistent_was_set_explicitly():
            return
        warnings.warn(
            "No cluster defined; running without persistence (define a cluster when using 'execution: remote')",
            stacklevel=0,
        )


def _is_seamless_worker() -> bool:
    module = sys.modules.get("seamless")
    if module is None:
        return False
    is_worker = getattr(module, "is_worker", None)
    if callable(is_worker):
        try:
            return bool(is_worker())
        except Exception:
            return False
    return False


def change_stage():
    from .select import get_selected_cluster
    from .cluster import get_cluster, get_local_cluster
    from .select import get_execution, get_persistent

    global _initialized

    persistent = get_persistent()
    try:
        from .pure_daskserver import deactivate as pure_deactivate
    except Exception:
        pure_deactivate = None
    if pure_deactivate is not None:
        pure_deactivate()

    if persistent:
        try:
            import seamless_remote.daskserver_remote

            seamless_remote.daskserver_remote.deactivate()
        except ImportError:
            pass
    else:
        module = sys.modules.get("seamless_remote.daskserver_remote")
        if module is not None:
            try:
                module.deactivate()
            except Exception:
                pass

    cluster = get_selected_cluster()
    if cluster is not None:
        execution = get_execution()
        if not persistent and execution == "remote":
            from .select import check_remote_redundancy

            remote = check_remote_redundancy(cluster)
            if remote != "daskserver":
                raise ConfigurationError(
                    "Pure Dask mode requires a daskserver remote target"
                )
            from .pure_daskserver import activate as pure_activate

            pure_activate()
        elif persistent:
            try:
                import seamless_remote
            except ImportError:  # seamless_remote was not installed
                pass
            else:
                import seamless_remote.buffer_remote
                import seamless_remote.database_remote
                import seamless_remote.jobserver_remote
                import seamless_remote.daskserver_remote

                from .select import check_remote_redundancy

                if execution == "remote":
                    remote = check_remote_redundancy(cluster)
                    seamless_remote.buffer_remote.activate()
                    seamless_remote.database_remote.activate()
                    if remote == "jobserver":
                        seamless_remote.jobserver_remote.activate()
                    elif remote == "daskserver":
                        seamless_remote.daskserver_remote.activate()
                else:
                    seamless_remote.buffer_remote.activate()
                    seamless_remote.database_remote.activate()

    if get_execution() == "spawn":
        from seamless.transformer import spawn

        local_cluster = get_cluster(get_local_cluster())
        spawn(local_cluster.workers)

    _initialized = True


def set_stage(
    stage: Optional[str] = None, substage: Optional[str] = None, *, workdir=_UNSET
):
    """
    Sets the current stage, (re)loading and (re)evaluating all configuration.

    Sets the workdir if not set previously.
    If no argument is provided, infer it from the caller.
    """
    from .config_files import load_config_files

    if _remote_clients_set:
        raise RuntimeError("remote clients already set; stage cannot be changed")

    from .select import (
        select_stage,
        get_stage,
        select_substage,
    )

    old_stage = get_stage()
    old_initialized = _initialized

    stage_change = old_stage != stage or not old_initialized

    select_stage(stage)
    select_substage(
        substage
    )  # TODO: re-evaluate job delegation after substage change? or do it dynamically, when the first job is submitted?

    if workdir is _UNSET and not _set_workdir_called:
        _set_workdir(_UNSET, 2)
    load_config_files()
    _report_execution_requirements()
    if stage_change:
        change_stage()


def set_substage(substage: Optional[str] = None):
    """
    Sets the current substage, (re)loading and (re)evaluating all configuration.
    """
    from .select import get_stage

    stage = get_stage()
    return set_stage(stage, substage)


def init(*, workdir=_UNSET):
    """
    Initializes the configuration, loading and evaluating all configuration.
    If init() or set_stage() were already called, do nothing.

    Sets the workdir if not set previously.
    If no argument is provided, infer it from the caller.
    """
    if _is_seamless_worker():
        return
    if _initialized:
        return
    if workdir is _UNSET and not _set_workdir_called:
        _set_workdir(_UNSET, 2)
    return set_stage(workdir=workdir)


from .extern_clients import collect_remote_clients, set_remote_clients

__all__ = [
    "init",
    "set_stage",
    "set_substage",
    "set_workdir",
    "collect_remote_clients",
    "set_remote_clients",
]
