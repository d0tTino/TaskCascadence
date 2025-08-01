import yaml
from task_cascadence.config import load_config
from task_cascadence.stage_store import StageStore
from task_cascadence.pointer_store import PointerStore
from task_cascadence.idea_store import IdeaStore


def test_config_paths(monkeypatch, tmp_path):
    cfg = tmp_path / "cfg.yml"
    stages = tmp_path / "stages.yml"
    pointers = tmp_path / "pointers.yml"
    ideas = tmp_path / "ideas.yml"
    cfg.write_text(
        yaml.safe_dump({
            "stages_path": str(stages),
            "pointers_path": str(pointers),
            "ideas_path": str(ideas),
        })
    )
    monkeypatch.setenv("CASCADENCE_CONFIG", str(cfg))
    monkeypatch.delenv("CASCADENCE_POINTERS_PATH", raising=False)
    monkeypatch.delenv("CASCADENCE_STAGES_PATH", raising=False)
    monkeypatch.delenv("CASCADENCE_IDEAS_PATH", raising=False)

    cfg_data = load_config()
    assert cfg_data["stages_path"] == str(stages)
    assert cfg_data["pointers_path"] == str(pointers)
    assert cfg_data["ideas_path"] == str(ideas)

    s_store = StageStore()
    p_store = PointerStore()
    i_store = IdeaStore()
    assert s_store.path == stages
    assert p_store.path == pointers
    assert i_store.path == ideas


def test_env_config_paths(monkeypatch, tmp_path):
    stages = tmp_path / "stages.yml"
    pointers = tmp_path / "pointers.yml"
    ideas = tmp_path / "ideas.yml"

    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(stages))
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(pointers))
    monkeypatch.setenv("CASCADENCE_IDEAS_PATH", str(ideas))
    monkeypatch.delenv("CASCADENCE_CONFIG", raising=False)

    cfg_data = load_config()

    assert cfg_data["stages_path"] == str(stages)
    assert cfg_data["pointers_path"] == str(pointers)
    assert cfg_data["ideas_path"] == str(ideas)

    s_store = StageStore()
    p_store = PointerStore()
    i_store = IdeaStore()

    assert s_store.path == stages
    assert p_store.path == pointers
    assert i_store.path == ideas
