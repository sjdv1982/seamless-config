import inspect
import os
from typing import Optional


class ConfigurationError(RuntimeError):
    pass


_workdir = None
_set_workdir_called = False
_initialized = False
_UNSET = object()


def get_workdir():
    """Return the configured workdir or fall back to the current directory."""
    return _workdir if _workdir is not None else os.getcwd()


def _set_workdir(workdir, nback):
    """
    Optionally set the workdir. If no argument is provided, infer it from the caller.
    """
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
    return _set_workdir(workdir, 1)


def set_stage(
    stage: Optional[str] = None, substage: Optional[str] = None, *, workdir=_UNSET
):
    """
    Sets the current stage, (re)loading and (re)evaluating all configuration.

    Sets the workdir if not set previously.
    If no argument is provided, infer it from the caller.
    """
    from .config_files import load_config_files
    from .select import (
        select_stage,
        get_stage,
        select_substage,
    )

    global _initialized
    old_stage = get_stage()
    old_initialized = _initialized

    stage_change = old_stage != stage or not old_initialized

    _initialized = True

    select_stage(stage)
    select_substage(
        substage
    )  # TODO: re-evaluate job delegation after substage change? or do it dynamically, when the first job is submitted?
    if workdir is _UNSET and not _set_workdir_called:
        _set_workdir(_UNSET, 2)
    result = load_config_files()
    if stage_change:
        try:
            import seamless_remote
        except ImportError:  # seamless_remote was not installed
            pass
        else:
            import seamless_remote.buffer_remote

            seamless_remote.buffer_remote.activate()

    return result


def set_substage(substage: Optional[str] = None):
    """
    Sets the current substage, (re)loading and (re)evaluating all configuration.
    """
    from .select import get_stage

    stage = get_stage()
    if not _set_workdir_called:
        _set_workdir(_UNSET, 2)
    return set_stage(stage, substage)


def init(*, workdir=_UNSET):
    """
    Initializes the configuration, loading and evaluating all configuration.
    If init() or set_stage() were already called, do nothing.

    Sets the workdir if not set previously.
    If no argument is provided, infer it from the caller.
    """
    if _initialized:
        return
    if workdir is _UNSET and not _set_workdir_called:
        _set_workdir(_UNSET, 2)
    return set_stage()


__all__ = [init, set_stage, set_substage, set_workdir]
