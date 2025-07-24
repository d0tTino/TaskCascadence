import subprocess
import sys


def test_mypy_runs() -> None:
    result = subprocess.run([sys.executable, "-m", "mypy", "."], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr

