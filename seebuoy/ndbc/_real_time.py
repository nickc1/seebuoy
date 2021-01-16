import pandas as pd
import requests
from io import StringIO
from ._request import make_request

BASE_URL = "http://www.ndbc.noaa.gov/data/realtime2"


def real_time(buoy, dataset="txt"):
    """Get realtime data from the NDBC. There are six different data sources
    to pull from:

    - data_spec: Raw Spectral Wave Data
    - ocean: Oceanographic Data
    - spec: Spectral Wave Summary Data
    - supl: Supplemental Measurements Data
    - swdir: Spectral Wave Data (alpha1)
    - swdir2: Spectral Wave Data (alpha2)
    - swr1: Spectral Wave Data (r1)
    - swr2: Spectral Wave Data (r2)
    - txt: Standard Meteorological Data

    Example usage:

        df = ndbc(41013, 'txt')

    Args:
        buoy (int): buoy id
        dataset (str): What type of data to query. Possible values are:
            'data_spec', 'ocean', 'spec', 'supl', 'swdir', 'swdir2', 'swr1',
            'swr2', and 'txt'

    Returns:
        DataFrame containing the requested data.
    """

    opts = {
        "data_spec": _data_spec,
        "ocean": _ocean,
        "spec": _spec,
        "supl": _supl,
        "swdir": _swdir,
        "swdir2": _swdir2,
        "swr1": _swr1,
        "swr2": _swr2,
        "txt": _txt,
    }

    if dataset not in opts:
        raise ValueError("Dataset must be one of {}".format(", ".join(opts)))

    url = f"{BASE_URL}/{buoy}.{dataset}"
    txt = make_request(url)

    if txt is None:
        return
    df = opts[dataset](txt)
    df.columns = df.columns.str.lower()
    return df


def _data_spec(txt):

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


def _ocean(txt):

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

    return df


def _spec(txt):

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


def _supl(txt):

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


def _swdir(txt):

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


def _swdir2(txt):

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


def _swr1(txt):

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


def _swr2(txt):

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


def _txt(txt):

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

    return df
