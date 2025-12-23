import importlib
import sys


def _purge(prefix: str) -> None:
    for name in list(sys.modules):
        if name == prefix or name.startswith(prefix + "."):
            sys.modules.pop(name, None)


def test_import_seamless_config_does_not_load_optional_modules():
    for mod in ("seamless_config", "seamless_remote", "seamless_transformer", "seamless"):
        _purge(mod)

    importlib.import_module("seamless_config")

    assert "seamless_remote" not in sys.modules
    assert "seamless_transformer" not in sys.modules
    assert "seamless" not in sys.modules
