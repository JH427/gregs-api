import subprocess
import time
from dataclasses import dataclass

from board_worker.config import BoardWorkerConfig


@dataclass
class HermesExecutionResult:
    stdout: str
    stderr: str
    exit_code: int
    runtime_seconds: float
    status: str
    result: str


def _parse_output(stdout: str, exit_code: int) -> tuple[str, str]:
    status = "failed" if exit_code else "done"
    result = stdout.strip()
    for line in stdout.splitlines():
        if line.startswith("STATUS:"):
            status = line.split(":", 1)[1].strip()
        elif line.startswith("RESULT:"):
            result = line.split(":", 1)[1].strip()
    return status, result


def run_hermes(config: BoardWorkerConfig, prompt: str) -> HermesExecutionResult:
    started = time.time()
    proc = subprocess.run(
        [
            config.hermes_command,
            "-p",
            config.hermes_profile,
            "chat",
            "-q",
            prompt,
        ],
        capture_output=True,
        text=True,
    )
    runtime_seconds = time.time() - started
    status, result = _parse_output(proc.stdout, proc.returncode)
    return HermesExecutionResult(
        stdout=proc.stdout,
        stderr=proc.stderr,
        exit_code=proc.returncode,
        runtime_seconds=runtime_seconds,
        status=status,
        result=result,
    )
