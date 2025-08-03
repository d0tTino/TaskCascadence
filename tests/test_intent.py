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
    assert 'send email to [REDACTED]' in captured['prompt']


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


def test_sanitization_prevents_injection(monkeypatch):
    captured = {}

    def fake_gather(prompt: str):
        captured['prompt'] = prompt
        return {'task': 'noop', 'arguments': {}, 'confidence': 1.0}

    monkeypatch.setattr('task_cascadence.intent.gather', fake_gather)
    client = TestClient(app)
    malicious = "<script>alert('x')</script>; rm -rf / 4111111111111111 bob@example.com"
    resp = client.post('/intent', json={'message': malicious, 'context': []})
    assert resp.status_code == 200
    prompt = captured['prompt']
    assert '<script>' not in prompt
    assert 'rm -rf' not in prompt
    assert 'bob@example.com' not in prompt
    assert '4111111111111111' not in prompt
    assert '[REDACTED]' in prompt


def test_sanitize_input_function():
    from task_cascadence.intent import sanitize_input

    text = "Contact me at alice@example.com using card 4242-4242-4242-4242 <script>bad()</script>"
    sanitized = sanitize_input(text)
    assert 'alice@example.com' not in sanitized
    assert '4242-4242-4242-4242' not in sanitized
    assert 'bad()' not in sanitized
    assert '[REDACTED]' in sanitized
