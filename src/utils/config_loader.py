import yaml
from pathlib import Path

def load_config(config_path="config/config.yaml"):
    """
    Load configuration from YAML file.
    config_path: Path to config.yaml
    """
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        raise RuntimeError(f"Failed to load config {config_path}: {e}")