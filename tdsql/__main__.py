from pathlib import Path

from tdsql.command import run
from tdsql.logger import logger

if __name__ == "__main__":
    yamlpath = Path("tdsql.yaml")
    ymlpath = Path("tdsql.yml")

    if yamlpath.is_file():
        run(yamlpath)

    elif ymlpath.is_file():
        run(ymlpath)

    else:
        logger.error("tdsql.yaml is not found")
        exit(code=1)
