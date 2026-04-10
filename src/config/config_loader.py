import yaml
from pathlib import Path

def load_config(env: str = "dev") -> dict: 

    """Loads environment-specific YAML config from the config directory.

    Defaults to 'dev' environment. For other environments, pass 'test' or 'prod'.
    Returns a dict with paths, table names, API parameters, etc.
    """

    config_path = Path(__file__).parent / f"{env}.yaml" 
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
