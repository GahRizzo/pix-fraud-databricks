from pathlib import Path
import yaml


def load_yaml(path: str) -> dict:
    with Path(path).open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)
