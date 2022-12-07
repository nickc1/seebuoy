import requests


DATASET_MAP = {
    "standard": "txt",
    "oceanographic": "ocean",
    "supplemental": "supl",
    "raw_spectral": "data_spec",
    "spectral_summary": "spec",
    "spectral_alpha1": "swdir",
    "spectral_alpha2": "swdir2",
    "spectral_r1": "swr1",
    "spectral_r2": "swr2",
}

BASE_URL = "https://www.ndbc.noaa.gov/data"


def get_url(url):

    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.text
    elif resp.status_code == 404:
        print(f"Dataset not available (404 Error) for url: \n {url}")
        return None
    else:
        raise ValueError(f"Error code {resp.status_code} for url: \n {url}")