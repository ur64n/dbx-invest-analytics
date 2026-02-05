import yaml
from pathlib import Path

def load_config(env: str = "dev") -> dict: 

    """
    Load environment configuration.
    """
    config_path = Path(__file__).parent / f"{env}.yaml" 
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
    

#Zwraca slownik z klucz:wartosc, na podstawie pliku config dev.yaml, gdy nie podamy przy wywolaniu srodowiska domyslne jest "dev"

#Aby uruchomić na innym  środowisku wystarczy w pipeline uruchomić load_config("test/prod")

# config_path = buduje scieżkę do folderu config i "dokleja" env.yaml czyli plik srodowiskowy config, następnie go bezpiecznie otwiera i zamyka po zakonczeniu uzywania