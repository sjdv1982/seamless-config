import inspect
import os


class ConfigurationError(RuntimeError):
    pass


_workdir = None
_UNSET = object()


def get_workdir():
    """Return the configured workdir or fall back to the current directory."""
    return _workdir if _workdir is not None else os.getcwd()


def set_workdir(workdir=_UNSET):
    """
    Optionally set the workdir. If no argument is provided, infer it from the caller.
    """
    global _workdir
    if workdir is not _UNSET:
        if workdir is None:
            _workdir = None
        else:
            _workdir = os.path.abspath(os.fspath(workdir))
        return

    frame = inspect.currentframe()
    caller_frame = frame.f_back if frame is not None else None
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


from .config_files import load_config_files, load_tools
from .select import (
    select_stage,
    select_substage,
)

set_stage = select_stage
set_substage = select_substage
__all__ = [select_stage, select_substage, set_stage, set_substage, load_config_files]
