from pathlib import Path

def read_file(filepath: Path) -> str:
    with open(filepath, 'r') as file:
        res = file.read()
    return res

