import pandas as pd
import requests
from io import StringIO
from .. import extract

STANDARD_MAP = {
    'wdir': 'wind_direction',
    'wspd': 'wind_speed',
    'gst': 'wind_gust',
    'wvht': 'wave_height',
    'dpd': 'dominant_period',
    'apd': 'average_period',
    'mwd': 'mean_wave_direction',
    'pres': 'pressure',
    'atmp': 'air_temp',
    'wtmp': 'water_temp',
    'dewp': 'dewpoint',
    'vis': 'visibility',
    'ptdy': 'pressure_tendency',
    'tide': 'tide',
}

OCEANOGRAPHIC_MAP = {
    'depth': 'depth',
    'otmp': 'ocean_temp',
    'cond': 'conductivity',
    'sal': 'salinity',
    'o2%': 'dissolved_o2_perc',
    'o2ppm': 'dissolved_o2_ppm',
    'clcon': 'cholorophyll',
    'turb': 'turbidity',
    'ph': 'ph',
    'eh': 'redox',
}

def parse_avail_recent_datasets(txt):

    df = pd.read_html(txt)[0]
    df = df.dropna(subset=["Last modified"])
    
    col_rename = {
        "Name": "file_name",
        "Last modified": "last_modified",
        "Size": "size",
        "Description": "description"
    }
    
    df = df[list(col_rename)].rename(columns=col_rename)
    
    df["buoy_id"] = df["file_name"].str.split('.').str[0]
    df["file_extension"] = df["file_name"].str.split('.').str[1]

    mapper = {v:k for k, v in extract.RECENT_DATASETS.items()}
    df["dataset"] = df["file_extension"].map(mapper)

    df["url"] = "realtime2/" + df["file_name"]
    return df

def standard(txt, rename_cols=True):
    """Parse the dataset that ends in txt"""

    df = pd.read_csv(
        StringIO(txt),
        delim_whitespace=True,
        na_values="MM",
        parse_dates=[[0, 1, 2, 3, 4]],
        index_col=0,
    )

    # first column is units, so drop it
    df.drop(df.index[0], inplace=True)

    # convert the dates to datetimes
    df.index = pd.to_datetime(df.index, format="%Y %m %d %H %M")
    df.index.name = "date"

    for c in df.columns:
        df[c] = pd.to_numeric(df[c])

    if rename_cols:
        df.columns = df.columns.str.lower().map(STANDARD_MAP)
    return df


def oceanographic(txt, rename_cols=True):
    """Parse the dataset that ends in ocean"""

    df = pd.read_csv(
        StringIO(txt),
        delim_whitespace=True,
        na_values="MM",
        parse_dates=[[0, 1, 2, 3, 4]],
        index_col=0,
    )

    # units are in the second row drop them
    df.drop(df.index[0], inplace=True)

    # convert the dates to datetimes
    df.index = pd.to_datetime(df.index, format="%Y %m %d %H %M")
    df.index.name = "date"

    # convert to floats
    cols = ["DEPTH", "OTMP", "COND", "SAL"]
    df[cols] = df[cols].astype(float)

    if rename_cols:
        df.columns = df.columns.str.lower().map(OCEANOGRAPHIC_MAP)
    
    return df


def supplemental(txt):
    """Parse the dataset that ends in supl."""

    df = pd.read_csv(
        StringIO(txt),
        delim_whitespace=True,
        na_values="MM",
        parse_dates=[[0, 1, 2, 3, 4]],
        index_col=0,
    )

    # units are in the second row drop them
    df.drop(df.index[0], inplace=True)

    # convert the dates to datetimes
    df.index = pd.to_datetime(df.index, format="%Y %m %d %H %M")

    # convert to floats
    cols = ["PRES", "PTIME", "WSPD", "WDIR", "WTIME"]
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def raw_spectral(txt):
    """Parse the dataset that ends in data_spec."""

    df = pd.read_csv(
        StringIO(txt),
        delim_whitespace=True,
        skiprows=1,
        header=None,
        parse_dates=[[0, 1, 2, 3, 4]],
        index_col=0,
    )

    # convert the dates to datetimes
    df.index = pd.to_datetime(df.index, format="%Y %m %d %H %M")
    df.index.name = "date"

    specs = df.iloc[:, 1::2]
    freqs = df.iloc[0, 2::2]

    specs.columns = freqs

    # remove the parenthesis from the column index
    specs.columns = [c.replace("(", "").replace(")", "") for c in specs.columns]

    return specs


def spectral_summary(txt):
    """Parse the dataset that ends in spec."""

    df = pd.read_csv(
        StringIO(txt),
        delim_whitespace=True,
        na_values="MM",
        parse_dates=[[0, 1, 2, 3, 4]],
        index_col=0,
    )

    try:
        # units are in the second row drop them
        # df.columns = df.columns + '('+ df.iloc[0] + ')'
        df.drop(df.index[0], inplace=True)

        # convert the dates to datetimes
        df.index = pd.to_datetime(df.index, format="%Y %m %d %H %M")

        # convert to floats
        cols = ["WVHT", "SwH", "SwP", "WWH", "WWP", "APD", "MWD"]
        df[cols] = df[cols].astype(float)
    except IndexError:

        # convert the dates to datetimes
        df.index = pd.to_datetime(df.index, format="%Y %m %d %H %M")

        # convert to floats
        cols = ["H0", "SwH", "SwP", "WWH", "WWP", "AVP", "MWD"]
        df[cols] = df[cols].astype(float)
    df.index.name = "date"
    return df


def spectral_alpha1(txt):
    """Parse the dataset that ends in swdir."""
    df = pd.read_csv(
        StringIO(txt),
        delim_whitespace=True,
        skiprows=1,
        na_values=999,
        header=None,
        parse_dates=[[0, 1, 2, 3, 4]],
        index_col=0,
    )

    # convert the dates to datetimes
    df.index = pd.to_datetime(df.index, format="%Y %m %d %H %M")
    df.index.name = "date"

    specs = df.iloc[:, 0::2]
    freqs = df.iloc[0, 1::2]

    specs.columns = freqs

    # remove the parenthesis from the column index
    specs.columns = [cname.replace("(", "").replace(")", "") for cname in specs.columns]

    return specs


def spectral_alpha2(txt):
    """Parse the dataset that ends in swdir2."""

    df = pd.read_csv(
        StringIO(txt),
        delim_whitespace=True,
        skiprows=1,
        header=None,
        parse_dates=[[0, 1, 2, 3, 4]],
        index_col=0,
    )

    # convert the dates to datetimes
    df.index = pd.to_datetime(df.index, format="%Y %m %d %H %M")
    df.index.name = "date"

    specs = df.iloc[:, 0::2]
    freqs = df.iloc[0, 1::2]

    specs.columns = freqs

    # remove the parenthesis from the column index
    specs.columns = [cname.replace("(", "").replace(")", "") for cname in specs.columns]

    return specs


def spectral_r1(txt):
    """Parse the dataset that ends in swr1."""

    df = pd.read_csv(
        StringIO(txt),
        delim_whitespace=True,
        skiprows=1,
        header=None,
        parse_dates=[[0, 1, 2, 3, 4]],
        index_col=0,
        na_values=999,
    )

    # convert the dates to datetimes
    df.index = pd.to_datetime(df.index, format="%Y %m %d %H %M")
    df.index.name = "date"

    specs = df.iloc[:, 0::2]
    freqs = df.iloc[0, 1::2]

    specs.columns = freqs

    # remove the parenthesis from the column index
    specs.columns = [cname.replace("(", "").replace(")", "") for cname in specs.columns]

    return specs


def spectral_r2(txt):
    """Parse the dataset that ends in swr2"""

    df = pd.read_csv(
        StringIO(txt),
        delim_whitespace=True,
        skiprows=1,
        header=None,
        parse_dates=[[0, 1, 2, 3, 4]],
        index_col=0,
        na_values=999,
    )

    # convert the dates to datetimes
    df.index = pd.to_datetime(df.index, format="%Y %m %d %H %M")
    df.index.name = "date"
    specs = df.iloc[:, 0::2]
    freqs = df.iloc[0, 1::2]

    specs.columns = freqs

    # remove the parenthesis from the column index
    specs.columns = [cname.replace("(", "").replace(")", "") for cname in specs.columns]

    return specs
