from fastapi.testclient import TestClient

from task_cascadence.api import app


def test_disambiguation_with_context(monkeypatch):
    captured = {}

    def fake_gather(prompt: str):
        captured['prompt'] = prompt
        return {
            'task': 'send_email',
            'arguments': {'recipient': 'bob@example.com'},
            'confidence': 0.9,
        }

    monkeypatch.setattr('task_cascadence.intent.gather', fake_gather)
    client = TestClient(app)
    resp = client.post(
        '/intent',
        json={'message': 'Schedule it for tomorrow', 'context': ['send email to bob@example.com']},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data['task'] == 'send_email'
    assert data['confidence'] == 0.9
    assert not data['clarification']
    assert 'send email to bob@example.com' in captured['prompt']


def test_clarification_triggered(monkeypatch):
    def fake_gather(prompt: str):
        return {'task': None, 'arguments': {}, 'confidence': 0.2}

    monkeypatch.setattr('task_cascadence.intent.gather', fake_gather)
    client = TestClient(app)
    resp = client.post('/intent', json={'message': 'do something', 'context': []})
    assert resp.status_code == 200
    data = resp.json()
    assert data['clarification']
    assert data['confidence'] == 0.2
