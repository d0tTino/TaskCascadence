import yaml
import importlib

from task_cascadence import pointer_sync


def test_pointer_sync_grpc(monkeypatch, tmp_path):
    store = tmp_path / "pointers.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store))
    monkeypatch.setenv("UME_TRANSPORT", "grpc")
    monkeypatch.setenv("UME_GRPC_METHOD", "Subscribe")

    module = tmp_path / "stub.py"
    module.write_text(
        """
class Stub:
    @staticmethod
    def Subscribe():
        from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate
        return [PointerUpdate(task_name='demo', run_id='r1', user_hash='u')]
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("UME_GRPC_STUB", "stub:Stub")

    importlib.invalidate_caches()
    pointer_sync.run()

    data = yaml.safe_load(store.read_text())
    assert data["demo"] == [{"run_id": "r1", "user_hash": "u"}]


def test_pointer_sync_nats(monkeypatch, tmp_path):
    store = tmp_path / "pointers.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store))
    monkeypatch.setenv("UME_TRANSPORT", "nats")

    module = tmp_path / "conn.py"
    module.write_text(
        """
class Conn:
    def subscribe_sync(self, subject):
        from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate
        update = PointerUpdate(task_name='demo', run_id='r2', user_hash='x')
        return [update.SerializeToString()]
conn = Conn()
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("UME_NATS_CONN", "conn:conn")
    monkeypatch.setenv("UME_NATS_SUBJECT", "events")

    importlib.invalidate_caches()
    pointer_sync.run()

    data = yaml.safe_load(store.read_text())
    assert data["demo"] == [{"run_id": "r2", "user_hash": "x"}]


