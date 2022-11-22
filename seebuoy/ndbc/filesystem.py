import pandas as pd

def avail_recent_datasets():
    """Checks the ndbc filestystem to see what files are available."""
    
    df = pd.read_html("https://www.ndbc.noaa.gov/data/realtime2/")[0]
    df = df.dropna(subset=["Last modified"])
    
    col_rename = {
        "Name": "name",
        "Last modified": "last_modified",
        "Size": "size",
        "Description": "description"
    }
    
    df = df[list(col_rename)].rename(columns=col_rename)
    
    df["buoy_id"] = df["name"].str.split('.').str[0]
    df["dataset"] = df["name"].str.split('.').str[1]

    return df


def avail_historical_datasets(dataset=None):
    """
    adcp: Acoustic Doppler Current Profiler Current Year Historical Data
    adcp2: Acoustic Doppler Current Profiler Current Year Historical Data
    cwind: Continuous Winds Current Year Historical Data
    dart: Water Column Height (DART) Current Year Historical Data
    mmbcur: No description available 
    ocean: Oceanographic Current Year Historical Data
    rain: Hourly Rain Current Year Historical Data
    rain10: 10 :Minute Rain Current Year Historical Data
    rain24: 24 :Hour Rain Current Year Historical Data
    srad: Solar Radiation Current Year Historical Data
    stdmet: Standard Meteorological Current Year Historical Data
    supl: Supplemental Measurements Current Year Historical Data
    swden: Raw Spectral Wave Current Year Historical Data
    swdir: Spectral Wave Current Year Historical Data (alpha1)
    swdir2: Spectral Wave Current Year Historical Data (alpha2)
    swr1: Spectral Wave Current Year Historical Data (r1)
    swr2: Spectral Wave Current Year Historical Data (r2)
    wlevel: Tide Current Year Historical Data
    """

    base_url = "https://www.ndbc.noaa.gov/data/historical"

    if dataset == "stdmet"
        url = f"{base_url}/stdmet"
        df_raw = pd.read_html(url)[0]
    

    df = df_raw.dropna(subset=["Last modified"])
    col_rename = {
        "Name": "name",
        "Last modified": "last_modified",
        "Size": "size",
        "Description": "description"
    }
    df = df[list(col_rename)].rename(columns=col_rename)
    