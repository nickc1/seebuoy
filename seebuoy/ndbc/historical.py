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
    "standard": "stdmet",
    "supplemental": "supl",
    "raw_spectral": "swden",
    "spectral_alpha1": "swdir",
    "spectral_alpha2": "swdir2",
    "spectral_r1": "swr1",
    "spectral_r2": "swr2",
    "tide": "wlevel",
}


STANDARD_MAP = {
    "wd": "wind_direction",  # older version
    "wdir": "wind_direction",
    "wspd": "wind_speed",
    "gst": "wind_gust",
    "wvht": "wave_height",
    "dpd": "dominant_period",
    "apd": "average_period",
    "mwd": "mean_wave_direction",
    "bar": "pressure",  # older version
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

def extract_avail_historical(dataset):

    dataset_code = DATASETS[dataset]

    base_url = f"{utils.BASE_URL}/historical"
    url = f"{base_url}/{dataset_code}"
    txt = utils.get_url(url)

    return txt


# TRANSFORM

def _build_txt_url(name, suffix):
    base_url = "https://www.ndbc.noaa.gov/view_text_file.php?filename"
    url = f"{base_url}={name}&dir=data/historical/{suffix}/"

    return url


def parse_avail_historical(txt, dataset):
    dataset_code = DATASETS[dataset]

    df_raw = pd.read_html(txt)[0]

    df = df_raw.dropna(subset=["Last modified"])
    col_rename = {
        "Name": "file_name",
        "Last modified": "last_modified",
        "Size": "size",
        "Description": "description",
    }
    df = df[list(col_rename)].rename(columns=col_rename)

    # Example file name: 42007h1989.txt.gz
    df["station_id"] = df["file_name"].str.split(".").str[0].str[:-5]
    df["file_year"] = df["file_name"].str.split(".").str[0].str[-4:]

    df["url"] = f"historical/{dataset_code}/" + df["file_name"]
    df["dataset_code"] = dataset_code
    df["dataset"] = dataset
    df["timeframe"] = "historical"

    # https://www.ndbc.noaa.gov/view_text_file.php?filename=41037h2005.txt.gz&dir=data/historical/stdmet/
    df["txt_url"] = df.apply(
        lambda row: _build_txt_url(row["file_name"], row["dataset_code"]), axis=1
    )

    return df



def base_parser(txt):
    df = pd.read_csv(
        StringIO(txt),
        header=0,
        delim_whitespace=True,
        dtype=str,
        na_values=["99", "99.0", "99.00", "999.0"],
    )

    # first row is units, so drop it. data after 2007 has units
    if df.iloc[0, 0] == "#yr":
        df = df.drop(df.index[0])

    # data after 2007 has a minute columns
    if "mm" in df.columns:
        res = df.iloc[:, :5].agg("-".join, axis=1)
        df["date"] = pd.to_datetime(res, format="%Y-%m-%d-%H-%M")
        df = df.iloc[:, 5:]
    else:
        try:
            res = df.iloc[:, :4].agg("-".join, axis=1)
            df["date"] = pd.to_datetime(res, format="%Y-%m-%d-%H")
            df = df.iloc[:, 4:]
        except:
            # really old data uses two year e.g. 73 instead of 1973
            res = df.iloc[:, :4].agg("-".join, axis=1)
            df["date"] = pd.to_datetime(res, format="%y-%m-%d-%H")
            df = df.iloc[:, 4:]

    df = df.set_index("date")
    df = df.astype(float)
    return df


def parse_standard(txt, rename_cols=True):
    """Parses the filed ending in stdmet."""

    df = base_parser(txt)

    if rename_cols:
        df.columns = df.columns.str.lower()
        df = df.rename(columns=STANDARD_MAP)
    return df.astype(float)


def parse_oceanographic(txt, rename_cols=True):
    """Parses the filed ending in stdmet."""

    df = base_parser(txt)

    if rename_cols:
        df.columns = df.columns.str.lower()
        df = df.rename(columns=OCEANOGRAPHIC_MAP)
    return df.astype(float)


def parse_supplemental(txt, rename_cols=True):
    """Parses the filed ending in stdmet."""

    df = base_parser(txt)

    if rename_cols:
        df.columns = df.columns.str.lower()
        df = df.rename(columns=SUPPLEMENTAL_MAP)
    return df.astype(float)


def parse_raw_spectral(txt):
    df = base_parser(txt)
    return df


def parse_spectral_alpha1(txt):

    df = base_parser(txt)

    return df


def parse_spectral_alpha2(txt):

    df = base_parser(txt)

    return df


def parse_spectral_r1(txt):

    df = base_parser(txt)

    return df


def parse_spectral_r2(txt):

    df = base_parser(txt)

    return df


def parse_tide(txt):

    df = pd.read_csv(
        StringIO(txt),
        header=0,
        delim_whitespace=True,
        na_values=[99, 999, 9999, 99.0, 99.00, 999.0, 9999.0, "99", "99.0", "99.00"],
    )

    # first row is units, so drop it. data after 2007 has units
    if df.iloc[0, 0] == "#yr":
        df = df.drop(df.index[0])

    # data before 2007 does not always have minute
    if "mm" in df.columns:
        res = df.iloc[:, :5].astype(str).agg("-".join, axis=1)
        df["date"] = pd.to_datetime(res, format="%Y-%m-%d-%H-%M")
        df = df.iloc[:, 5:]
    else:
        # old data has years like 97 instead of 1997
        df["YY"] = "19" + df["YY"].astype(str)
        res = df.iloc[:, :4].astype(str).agg("-".join, axis=1)
        df["date"] = pd.to_datetime(res, format="%Y-%m-%d-%H")
        df = df.iloc[:, 4:]

    df = df.set_index("date")
    df.columns = df.columns.str.lower()

    return df

# MAIN INTERFACE

def avail_historical(dataset):
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

    if dataset == "all":
        datasets = list(DATASETS)
    else:
        datasets = [dataset]

    df_store = []
    for ds in datasets:
        
        txt = extract_avail_historical(ds)
        df = parse_avail_historical(txt, ds)
        df_store.append(df)

    return pd.concat(df_store)


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

    # elif dataset == "spectral_summary":
    #     df = parse_spectral_summary(txt)

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