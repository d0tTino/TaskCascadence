import yaml
from task_cascadence.config import load_config
from task_cascadence.stage_store import StageStore
from task_cascadence.pointer_store import PointerStore


def test_config_paths(monkeypatch, tmp_path):
    cfg = tmp_path / "cfg.yml"
    stages = tmp_path / "stages.yml"
    pointers = tmp_path / "pointers.yml"
    cfg.write_text(yaml.safe_dump({"stages_path": str(stages), "pointers_path": str(pointers)}))
    monkeypatch.setenv("CASCADENCE_CONFIG", str(cfg))
    monkeypatch.delenv("CASCADENCE_POINTERS_PATH", raising=False)
    monkeypatch.delenv("CASCADENCE_STAGES_PATH", raising=False)

    cfg_data = load_config()
    assert cfg_data["stages_path"] == str(stages)
    assert cfg_data["pointers_path"] == str(pointers)

    s_store = StageStore()
    p_store = PointerStore()
    assert s_store.path == stages
    assert p_store.path == pointers
