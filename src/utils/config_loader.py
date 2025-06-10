from ruamel.yaml import YAML
from pathlib import Path

def load_config(config_path: str = "config/config.yaml"):
    try:
        yaml = YAML(typ='safe')
        yaml.preserve_quotes = True
        yaml.indent(mapping=2, sequence=4, offset=2)
        with open(config_path, "r") as f:
            return yaml.load(f)
    except Exception as e:
        raise RuntimeError(f"Failed to load config {config_path}: {e}")