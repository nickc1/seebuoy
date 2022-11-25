from io import StringIO
import pandas as pd
from sklearn.neighbors import NearestNeighbors
from ..data import large_cities


def parse_buoy_owners(txt):
    
    df = pd.read_csv(
        StringIO(txt),
        sep='|',
        skiprows=1
    )
    df.columns = df.columns.str.replace('#', '').str.lower().str.strip()
    df["ownercode"] = df["ownercode"].str.strip()

    return df


def parse_buoy_locations(txt):

    
    df = pd.read_csv(
        StringIO(txt),
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

    return df


def add_closest_cities(df):
    
    df_cities = pd.DataFrame(large_cities)
    
    nbrs = NearestNeighbors(n_neighbors=1, algorithm='ball_tree').fit(df_cities[["latitude", "longitude"]].values)
    distances, indices = nbrs.kneighbors(df[["lat", "lon"]].values)

    df_closest_cities = df_cities.iloc[indices.flatten()].reset_index(drop=True)
    cols = {"city": "closest_city", "state": "closest_state"}
    df = df.join(df_closest_cities[list(cols)].rename(columns=cols))

    return df


def add_owners(df_buoys, df_owners):

    df = pd.merge(df_buoys, df_owners, left_on="owner", right_on="ownercode", how="left")

    return df
