from pathlib import Path
from typing import Union, Any

import yaml


def parse_yaml(path: Union[Path, str]) -> Any:
    with open(path) as file:
        return yaml.safe_load(file)
