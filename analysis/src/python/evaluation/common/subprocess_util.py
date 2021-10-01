import subprocess
from typing import List


def run_in_subprocess_with_working_dir(command: List[str], working_dir: str) -> str:
    process = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=working_dir,
    )

    stdout = process.stdout.decode()

    return stdout


def run_and_wait(command: List[str]) -> None:
    process = subprocess.Popen(command)
    process.wait()
