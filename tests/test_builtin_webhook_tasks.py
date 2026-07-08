from task_cascadence.plugins import GitHubWebhookTask, CalComWebhookTask


def test_github_webhook_task_parses_action():
    GitHubWebhookTask.events.clear()
    task = GitHubWebhookTask()
    task.handle_event("github", "issues", {"action": "closed"}, user_id="alice", group_id="eng")
    assert GitHubWebhookTask.events == ["closed"]


def test_calcom_webhook_task_parses_event():
    CalComWebhookTask.events.clear()
    task = CalComWebhookTask()
    task.handle_event("calcom", "booking", {"event": "created"}, user_id="alice", group_id="eng")
    assert CalComWebhookTask.events == ["created"]
