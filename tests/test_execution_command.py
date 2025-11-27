import warnings

import pytest

import seamless_config
import seamless_config.select as select


def _reset_state(monkeypatch):
    monkeypatch.setattr(seamless_config, "_initialized", False)
    monkeypatch.setattr(seamless_config, "_set_workdir_called", False)
    monkeypatch.setattr(seamless_config, "_remote_clients_set", False)
    monkeypatch.setattr(seamless_config, "_workdir", None)

    monkeypatch.setattr(select, "_current_cluster", None)
    monkeypatch.setattr(select, "_current_project", None)
    monkeypatch.setattr(select, "_current_subproject", None)
    monkeypatch.setattr(select, "_current_stage", None)
    monkeypatch.setattr(select, "_current_substage", None)
    monkeypatch.setattr(select, "_current_execution", "process")
    monkeypatch.setattr(select, "_execution_source", None)
    monkeypatch.setattr(select, "_execution_command_seen", False)


def test_execution_defaults_to_process(monkeypatch, tmp_path):
    _reset_state(monkeypatch)
    workdir = tmp_path / "workdir"
    workdir.mkdir()
    monkeypatch.setenv("HOME", str(tmp_path))

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        seamless_config.set_workdir(workdir)
        seamless_config.init()

    messages = [str(w.message) for w in caught]
    assert any("falls back to 'process'" in msg for msg in messages)
    assert any("No cluster defined; running without persistence" in msg for msg in messages)
    assert select.get_execution() == "process"


def test_remote_execution_requires_cluster(monkeypatch, tmp_path):
    _reset_state(monkeypatch)
    workdir = tmp_path / "remote-execution"
    workdir.mkdir()
    monkeypatch.setenv("HOME", str(tmp_path))
    (workdir / "seamless.yaml").write_text(
        "- execution: remote\n- project: demo\n", encoding="utf-8"
    )

    seamless_config.set_workdir(workdir)
    with pytest.raises(seamless_config.ConfigurationError):
        seamless_config.init()
