import json

import seamless_config
from seamless_config.extern_clients import collect_remote_clients


def test_collect_remote_clients_writes_json(tmp_path, monkeypatch):
    data = {
        "database": [{"readonly": True, "url": "http://db"}],
        "buffer": [
            {"readonly": True, "directory": "/buffers"},
            {"readonly": False, "url": "http://hash"},
        ],
    }

    class Dummy:
        pass

    from seamless_remote import database_remote, buffer_remote

    monkeypatch.setattr(database_remote, "inspect_extern_clients", lambda: [data["database"][0]])
    monkeypatch.setattr(database_remote, "inspect_launched_clients", lambda: [])
    monkeypatch.setattr(buffer_remote, "inspect_extern_clients", lambda: data["buffer"])
    monkeypatch.setattr(buffer_remote, "inspect_launched_clients", lambda: [])

    result = collect_remote_clients("dummy-cluster")
    path = tmp_path / "clients.json"
    path.write_text(json.dumps(result), encoding="utf-8")

    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded == data
