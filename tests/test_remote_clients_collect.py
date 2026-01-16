from copy import deepcopy
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
    data2 = deepcopy(data)
    for k in ("database",):
        for v in data2[k]:
            for kk in list(v.keys()):
                if kk == "url":
                    v["remote_url"] = v["url"]
                    del v["url"]

    class Dummy:
        pass

    from seamless_remote import database_remote, buffer_remote

    monkeypatch.setattr(
        database_remote, "inspect_extern_clients", lambda: [data2["database"][0]]
    )
    monkeypatch.setattr(database_remote, "inspect_launched_clients", lambda: [])
    monkeypatch.setattr(
        buffer_remote, "inspect_extern_clients", lambda: data2["buffer"]
    )
    monkeypatch.setattr(buffer_remote, "inspect_launched_clients", lambda: [])

    result = collect_remote_clients("dummy-cluster")
    path = tmp_path / "clients.json"
    path.write_text(json.dumps(result), encoding="utf-8")

    loaded = json.loads(path.read_text(encoding="utf-8"))
    print(loaded)
    assert loaded == data
