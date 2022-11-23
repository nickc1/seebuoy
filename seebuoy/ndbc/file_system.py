import requests
from io import StringIO
import pandas as pd
from sklearn.neighbors import NearestNeighbors

from .large_cities import large_cities

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

def add_closest_cities(df):
    
    df_cities = pd.DataFrame(large_cities)
    
    nbrs = NearestNeighbors(n_neighbors=1, algorithm='ball_tree').fit(df_cities[["latitude", "longitude"]].values)
    distances, indices = nbrs.kneighbors(df[["lat", "lon"]].values)

    df_closest_cities = df_cities.iloc[indices.flatten()].reset_index(drop=True)
    cols = {"city": "closest_city", "state": "closest_state"}
    df = df.join(df_closest_cities[list(cols)].rename(columns=cols))

    return df

def buoy_owners():

    url = "https://www.ndbc.noaa.gov/data/stations/station_owners.txt"
    resp = requests.get(url)
    df = pd.read_csv(
        StringIO(resp.text),
        sep='|',
        skiprows=1
    )
    df.columns = df.columns.str.replace('#', '').str.lower().str.strip()
    df["ownercode"] = df["ownercode"].str.strip()

    return df


def buoy_locations(closest_cities=True, owners=True):
    
    url = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"
    resp = requests.get(url)
    
    df = pd.read_csv(
        StringIO(resp.text),
        sep='|'
    )

    # parse out column names. First row is nans
    df.columns = df.columns.str.replace('#', '').str.lower().str.strip()
    df = df.iloc[1:].reset_index(drop=True)

    # parse lat and lon
    # 30.000 N 90.000 W (30&#176;0'0" N 90&#176;0'0" W)
    df["lat_lon"] = df["location"].str.split('(').str[0].str.strip()
    lat_lon = df["lat_lon"].str.split(" ", expand=True)
    lat_lon.columns = ["lat", "north_south", "lon", "east_west"]

    # convert southern/western latitudes to negatives
    m = lat_lon["north_south"] == "S"
    lat_lon.loc[m, "lat"] = lat_lon.loc[m, "lat"].astype(float) * -1

    m = lat_lon["east_west"] == "W"
    lat_lon.loc[m, "lon"] = lat_lon.loc[m, "lon"].astype(float) * -1

    df = df.join(lat_lon[["lat", "lon"]])

    if closest_cities:
        df = add_closest_cities(df)
    
    if owners:
        df_owners = buoy_owners()
        df = pd.merge(df, df_owners, left_on="owner", right_on="ownercode", how="left")

    return df


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


def build_txt_url(name, suffix):
    base_url = "https://www.ndbc.noaa.gov/view_text_file.php?filename"
    url = f"{base_url}={name}&dir=data/historical/{suffix}/"
    
    return url
    

def get_historical(dataset):
    
    suffix = HIST_DATASETS[dataset]

    base_url = "https://www.ndbc.noaa.gov/data/historical"
    url = f"{base_url}/{suffix}"

    df_raw = pd.read_html(url)[0]

    df = df_raw.dropna(subset=["Last modified"])
    col_rename = {
        "Name": "name",
        "Last modified": "last_modified",
        "Size": "size",
        "Description": "description"
    }
    df = df[list(col_rename)].rename(columns=col_rename)    

    # add metadata
    df["url"] = url
    df["dataset"] = dataset
    df["suffix"] = suffix

    # Example file name: 42007h1989.txt.gz
    df["compression"] = df["name"].str.split('.').str[-1]
    df["file_extension"] = df["name"].str.split('.').str[-2]
    df["file_name"] = df["name"].str.split('.').str[0]
    df["buoy_id"] = df["file_name"].str[:-5]
    df["file_year"] = df["file_name"].str[-4:]

    # https://www.ndbc.noaa.gov/view_text_file.php?filename=41037h2005.txt.gz&dir=data/historical/stdmet/
    df["txt_url"] = df.apply(lambda row: build_txt_url(row['name'], row['suffix']), axis=1)

    return df


def avail_historical_datasets(dataset=None):
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

    if dataset is None:

        df_store = []
        for dataset in list(HIST_DATASETS):
            print(f"Retrieving {dataset}.")
            df = get_historical(dataset)
            df_store.append(df)
        
        df = pd.concat(df_store)
    
    else:
        df = get_historical(dataset)

    return df
    