from __future__ import annotations

from pathlib import Path

import yaml  # type: ignore

from .tools import define_tools

TOOLS_FILENAME = "tools.yaml"


# Tool definition
def load_tools() -> dict:
    """
    Load tool definitions from tools.yaml and register them inside seamless_config.tools.
    """
    tools_file = Path(__file__).with_name(TOOLS_FILENAME)
    with tools_file.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if data is None:
        data = {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected a mapping in {tools_file}, found {type(data).__name__}")

    define_tools(data)
    return data
