from __future__ import annotations

from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver as Observer

from . import reload_plugins


class _ReloadHandler(FileSystemEventHandler):
    """Internal handler that reloads plugins on any file change."""

    def on_any_event(self, event):  # pragma: no cover - simple passthrough
        if event.is_directory:
            return
        reload_plugins()


class PluginWatcher:
    """Watch a directory for plugin changes and reload."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._observer = Observer()
        self._handler = _ReloadHandler()

    def start(self) -> None:
        """Start watching the directory."""
        self._observer.schedule(self._handler, str(self.path), recursive=True)
        self._observer.start()

    def stop(self) -> None:
        """Stop watching."""
        self._observer.stop()
        self._observer.join()
