from io import StringIO
import pandas as pd
from . import utils

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
    "standard": "txt",
    "supplemental": "supl",
    "spectral_alpha1": "swdir",
    "spectral_alpha2": "swdir2",
    "spectral_r1": "swr1",
    "spectral_r2": "swr2",
    "tide": "wlevel",
    "standard_drift": "drift",
    "raw_spectral": "spec",
    "spectral_summary": "data_spec"
}

STANDARD_MAP = {
    "wdir": "wind_direction",
    "wspd": "wind_speed",
    "gst": "wind_gust",
    "wvht": "wave_height",
    "dpd": "dominant_period",
    "apd": "average_period",
    "mwd": "mean_wave_direction",
    "pres": "pressure",
    "atmp": "air_temp",
    "wtmp": "water_temp",
    "dewp": "dewpoint",
    "vis": "visibility",
    "ptdy": "pressure_tendency",
    "tide": "tide",
}

OCEANOGRAPHIC_MAP = {
    "depth": "depth",
    "otmp": "ocean_temp",
    "cond": "conductivity",
    "sal": "salinity",
    "o2%": "dissolved_o2_perc",
    "o2ppm": "dissolved_o2_ppm",
    "clcon": "cholorophyll",
    "turb": "turbidity",
    "ph": "ph",
    "eh": "redox",
}

SUPPLEMENTAL_MAP = {
    "pres": "pressure",
    "ptime": "pressure_time",
    "wspd": "windspeed",
    "wdir": "wind_direction",
    "wtime": "wind_time",
}


# EXTRACT

def extract_avail_real_time():
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

    url = f"{utils.BASE_URL}/realtime2"
    txt = utils.get_url(url)

    return txt

def extract_station(station_id, dataset):

    dataset_code = utils.DATASETS[dataset]
    url = f'https://www.ndbc.noaa.gov/data/realtime2/{station_id}.{dataset_code}'

    txt = utils.get_url(url)

    return txt


# TRANSFORM
def parse_avail_real_time(txt):

    df = pd.read_html(txt)[0]
    df = df.dropna(subset=["Last modified"])

    col_rename = {
        "Name": "file_name",
        "Last modified": "last_modified",
        "Size": "size",
        "Description": "description",
    }

    df = df[list(col_rename)].rename(columns=col_rename)

    df["station_id"] = df["file_name"].str.split(".").str[0]
    df["dataset_code"] = df["file_name"].str.split(".").str[1]

    mapper = {v: k for k, v in DATASETS.items()}
    df["dataset"] = df["dataset_code"].map(mapper)

    df["url"] = "realtime2/" + df["file_name"]
    df["txt_url"] = utils.BASE_URL + "/" + df["url"]

    df["timeframe"] = "real_time"
    return df

# DATASET PARSERS

def parse_standard(txt, rename_cols=True):
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


def parse_oceanographic(txt, rename_cols=True):
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
        df.columns = df.columns.str.lower()
        df = df.rename(columns=OCEANOGRAPHIC_MAP)

    return df


def parse_supplemental(txt):
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


def parse_raw_spectral(txt):
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


def parse_spectral_summary(txt):
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


def parse_spectral_alpha1(txt):
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


def parse_spectral_alpha2(txt):
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


def parse_spectral_r1(txt):
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


def parse_spectral_r2(txt):
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

# MAIN INTERFACE

def avail_real_time(dataset="standard"):

    txt = extract_avail_real_time()
    df = parse_avail_real_time(txt)

    if dataset != "all":
        m = df["dataset"] == dataset
        df = df[m]

    return df

def get_dataset(txt_url, dataset, rename_cols=True):

    txt = utils.get_url(txt_url)

    if dataset == "standard":
        df = parse_standard(txt, rename_cols=rename_cols)

    elif dataset == "oceanographic":
        df = parse_oceanographic(txt, rename_cols=rename_cols)

    elif dataset == "supplemental":
        df = parse_supplemental(txt)

    elif dataset == "raw_spectral":
        df = parse_raw_spectral(txt)

    elif dataset == "spectral_summary":
        df = parse_spectral_summary(txt)

    elif dataset == "spectral_alpha1":
        df = parse_spectral_alpha1(txt)

    elif dataset == "spectral_alpha2":
        df = parse_spectral_alpha2(txt)

    elif dataset == "spectral_r1":
        df = parse_spectral_r1(txt)

    elif dataset == "spectral_r2":
        df = parse_spectral_r2(txt)
    else:
        raise ValueError(f"Dataset must be one of {list(DATASETS)}.")
    
    return df
