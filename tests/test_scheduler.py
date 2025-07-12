from datetime import datetime
from zoneinfo import ZoneInfo


from task_cascadence.scheduler import BaseScheduler


class DummyScheduler(BaseScheduler):
    def __init__(self):
        self.last_args = None
        self.last_kwargs = None

    def schedule_task(self, *args, **kwargs):
        self.last_args = args
        self.last_kwargs = kwargs
        # Simulate scheduling by returning a scheduled time
        tz = kwargs.get("timezone", ZoneInfo("UTC"))
        return datetime.now(tz)


def test_schedule_with_timezone():
    sched = DummyScheduler()
    tz = ZoneInfo("America/New_York")
    scheduled_time = sched.schedule_task("task", timezone=tz)

    assert isinstance(scheduled_time, datetime)
    assert sched.last_kwargs["timezone"] == tz
