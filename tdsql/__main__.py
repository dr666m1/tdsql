from pathlib import Path

from tdsql.command import run

if __name__ == "__main__":
    run(Path("tdsql.yaml"))
