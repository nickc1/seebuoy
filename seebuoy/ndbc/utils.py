import requests


DATASETS = {
    "adcp": "adcp",
    "adcp2": "adcp2",
    "continuous_wind": "cwind",
    "water_col_height": "dart",
    "mmbcur": "mmbcur",
    "oceanographic": "ocean",
    "rain_hourly": "rain",
    "rain_10_min": "rain10",
    "rain_24_hr": "rain24",
    "solar_radiation": "srad",
    "standard": "stdmet",
    "standard": "txt",
    "supplemental": "supl",
    "raw_spectral": "swden",
    "spectral_alpha1": "swdir",
    "spectral_alpha2": "swdir2",
    "spectral_r1": "swr1",
    "spectral_r2": "swr2",
    "tide": "wlevel",
    "standard_drift": "drift",
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