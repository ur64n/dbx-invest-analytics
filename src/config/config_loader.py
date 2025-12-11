import yaml
from config import settings_path

# Otwiera plik yaml
def load_config():
    with open(settings_path, "r") as file:
        return yaml.safe_load(file)

if __name__ == "__main__":
    config = load_config()
    print(config)
