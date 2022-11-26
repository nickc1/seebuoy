from io import StringIO
import pandas as pd
from .. import extract
from .recent import STANDARD_MAP

STANDARD_MAP = {
    'wd': 'wind_direction', # older version
    'wdir': 'wind_direction',
    'wspd': 'wind_speed',
    'gst': 'wind_gust',
    'wvht': 'wave_height',
    'dpd': 'dominant_period',
    'apd': 'average_period',
    'mwd': 'mean_wave_direction',
    'bar': 'pressure', # older version
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


def _build_txt_url(name, suffix):
    base_url = "https://www.ndbc.noaa.gov/view_text_file.php?filename"
    url = f"{base_url}={name}&dir=data/historical/{suffix}/"
    
    return url

def parse_avail_historical(txt, dataset):
    file_suffix = extract.HIST_DATASETS[dataset]
    
    df_raw = pd.read_html(txt)[0]

    df = df_raw.dropna(subset=["Last modified"])
    col_rename = {
        "Name": "file_name",
        "Last modified": "last_modified",
        "Size": "size",
        "Description": "description"
    }
    df = df[list(col_rename)].rename(columns=col_rename)    

    # Example file name: 42007h1989.txt.gz
    df["compression"] = df["file_name"].str.split('.').str[-1]
    df["file_extension"] = df["file_name"].str.split('.').str[-2]
    df["file_root"] = df["file_name"].str.split('.').str[0]
    df["station_id"] = df["file_root"].str[:-5]
    df["file_year"] = df["file_root"].str[-4:]

    df["url"] = f"historical/{file_suffix}/" + df["file_name"]
    df["file_suffix"] = file_suffix
    df["dataset"] = dataset

    # https://www.ndbc.noaa.gov/view_text_file.php?filename=41037h2005.txt.gz&dir=data/historical/stdmet/
    df["txt_url"] = df.apply(lambda row: _build_txt_url(row['file_name'], row['file_suffix']), axis=1)

    return df


def parse_all_avail_historical(data):

    df_store = []
    for dataset, txt in data.items():

        df = parse_avail_historical(txt, dataset)
        df_store.append(df)

    return pd.concat(df_store)


def standard(txt, rename_cols=True):
    """Parses the filed ending in stdmet."""

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
        res = df.iloc[:, :4].astype(str).agg("-".join, axis=1)
        df["date"] = pd.to_datetime(res, format="%Y-%m-%d-%H")
        df = df.iloc[:, 4:]

    df = df.set_index("date")

    if rename_cols:
        df.columns = df.columns.str.lower()
        df = df.rename(columns=STANDARD_MAP)
    return df.astype(float)


def oceanographic(txt, rename_cols=True):
    """Parses the filed ending in stdmet."""

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
        res = df.iloc[:, :4].astype(str).agg("-".join, axis=1)
        df["date"] = pd.to_datetime(res, format="%Y-%m-%d-%H")
        df = df.iloc[:, 4:]

    df = df.set_index("date")

    if rename_cols:
        df.columns = df.columns.str.lower()
        df = df.rename(columns=OCEANOGRAPHIC_MAP)
    return df.astype(float)