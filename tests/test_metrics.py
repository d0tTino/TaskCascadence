import pytest

from task_cascadence import metrics


def _hist_count(hist, name):
    for metric in hist.collect():
        for sample in metric.samples:
            if sample.name.endswith('_count') and sample.labels.get('task_name') == name:
                return sample.value
    return 0


def test_track_task_success(monkeypatch):
    @metrics.track_task
    def do_work():
        return 'ok'

    success = metrics.TASK_SUCCESS.labels('do_work')
    failure = metrics.TASK_FAILURE.labels('do_work')

    before_success = success._value.get()
    before_failure = failure._value.get()
    before_count = _hist_count(metrics.TASK_LATENCY, 'do_work')

    result = do_work()

    assert result == 'ok'
    assert success._value.get() == before_success + 1
    assert failure._value.get() == before_failure
    assert _hist_count(metrics.TASK_LATENCY, 'do_work') == before_count + 1


def test_track_task_failure():
    @metrics.track_task
    def boom():
        raise RuntimeError('fail')

    success = metrics.TASK_SUCCESS.labels('boom')
    failure = metrics.TASK_FAILURE.labels('boom')

    before_success = success._value.get()
    before_failure = failure._value.get()
    before_count = _hist_count(metrics.TASK_LATENCY, 'boom')

    with pytest.raises(RuntimeError):
        boom()

    assert success._value.get() == before_success
    assert failure._value.get() == before_failure + 1
    assert _hist_count(metrics.TASK_LATENCY, 'boom') == before_count + 1
