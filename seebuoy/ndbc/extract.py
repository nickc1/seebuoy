import requests
from io import StringIO
import pandas as pd


HIST_DATASETS = {
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
    "supplemental": "supl",
    "raw_spectral": "swden",
    "spectral_alpha1": "swdir",
    "spectral_alpha2": "swdir2",
    "spectral_r1": "swr1",
    "spectral_r2": "swr2",
    "tide": "wlevel",
}

RECENT_DATASETS ={
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


def get_url(url):

    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.text
    elif resp.status_code == 404:
        print(f"Dataset not available (404 Error) for url: \n {url}")
        return None
    else:
        raise ValueError(f"Error code {resp.status_code} for url: \n {url}")

# METADATA

def buoy_owners():

    url = "https://www.ndbc.noaa.gov/data/stations/station_owners.txt"
    txt = get_url(url)

    return txt


def buoy_locations():
    
    url = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"
    txt = get_url(url)

    return txt


# RECENT

def avail_recent_datasets():
    """All recent data (realtime) is put into the same folder. For example:
    realtime2/
        41013.data_spec
        41013.spec
        41013.supl
        41013.swdir
        41013.swdir2
        41013.swr1
        41013.swr2
        41013.txt
    """
    
    url = "https://www.ndbc.noaa.gov/data/realtime2/"
    txt = get_url(url)

    return txt

# CURRENT YEAR

def avail_current_year(dataset):
    suffix = HIST_DATASETS[dataset]
    months = [
        "Jan", 
        "Feb", 
        "Mar", 
        "Apr", 
        "May", 
        "Jun", 
        "Jul", 
        "Aug",
        "Sep", 
        "Oct", 
        "Nov", 
        "Dec"
        ]
    
    data = {}
    for month in months:
    
        url = f"https://www.ndbc.noaa.gov/data/{suffix}/{month}"
        txt = get_url(url)
        data[month] = txt

    return data


# HISTORICAL

def avail_historical(dataset):
    
    suffix = HIST_DATASETS[dataset]

    base_url = "https://www.ndbc.noaa.gov/data/historical"
    url = f"{base_url}/{suffix}"
    txt = get_url(url)

    return txt

def all_avail_historical():
    """
    adcp [adcp]: Acoustic Doppler Current Profiler Current Year Historical Data [adcp]
    adcp2 [adcp2]: Acoustic Doppler Current Profiler Current Year Historical Data [adcp2]
    continuous_wind: Continuous Winds Current Year Historical Data [cwind]
    water_col_height: Water Column Height (DART) Current Year Historical Data [dart]
    mmbcur: No description available [mmbcur]
    oceanographic: Oceanographic Current Year Historical Data [ocean]
    rain_hourly: Hourly Rain Current Year Historical Data [rain]
    rain_10_min: 10 Minute Rain Current Year Historical Data [rain10]
    rain_24_hr: 24 Hour Rain Current Year Historical Data [rain24]
    solar_radiation: Solar Radiation Current Year Historical Data [srad]
    standard: Standard Meteorological Current Year Historical Data [stdmet]
    supplemental: Supplemental Measurements Current Year Historical Data [supl]
    raw_spectral: Raw Spectral Wave Current Year Historical Data [swden]
    spectral_alpha1: Spectral Wave Current Year Historical Data (alpha1) [swdir]
    spectral_alpha2: Spectral Wave Current Year Historical Data (alpha2) [swdir2]
    spectral_r1: Spectral Wave Current Year Historical Data (r1) [swr1]
    spectral_r2: Spectral Wave Current Year Historical Data (r2) [swr2]
    tide: Tide Current Year Historical Data [wlevel]
    """

    txt_store = {}
    for dataset in list(HIST_DATASETS):
        print(f"Retrieving {dataset}.")
        txt = avail_historical(dataset)
        txt_store["dataset"] = txt

    return txt_store
    