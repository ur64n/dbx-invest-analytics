from urllib.parse import urlencode

# fred endpoints
SERIES_OBSERVATIONS = "series/observations"
SERIES_SEARCH = "series/search" 

def observations_url(series_id: str, start_date: str = None, end_date: str = None) -> str:
    """
    Generate URL for download macro indicators in XML.
    """
    params = {
        "series_id": series_id,
        "file_type": "xml"
    }
    if start_date:
        params["observation_start"] = start_date
    if end_date: # <- Aktualnie nie uzywane. (Jest na przyszlosc)
        params["observation_end"] = end_date

    return f"{SERIES_OBSERVATIONS}?{urlencode(params)}"

def search_url(search_text: str) -> str:
    """
    Generuje URL do wyszukiwania serii po tekście.
    """
    params = {
        "search_text": search_text,
        "file_type": "xml"
    }
    return f"{SERIES_SEARCH}?{urlencode(params)}"
