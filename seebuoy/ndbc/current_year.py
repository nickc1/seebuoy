from datetime import datetime
import pandas as pd
from . import historical
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
    "standard_drift": "drift",
}

MONTHS = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}

# EXTRACT

def extract_avail_current_year(dataset):

    dataset_code = DATASETS[dataset]

    data = {}
    for month in MONTHS:

        url = f"{utils.BASE_URL}/{dataset_code}/{month}"
        txt = utils.get_url(url)
        data[month] = txt

    return data


# TRANSFORM

def _build_txt_url(file_name, dataset_code, month):
    base_url = "https://www.ndbc.noaa.gov/view_text_file.php?filename"
    url = f"{base_url}={file_name}&dir=data/{dataset_code}/{month}/"

    return url


def parse_avail_current_year_month(txt, dataset, month):

    dataset_code = DATASETS[dataset]

    df = pd.read_html(txt)[0]
    col_rename = {
        "Name": "file_name",
        "Last modified": "last_modified",
        "Size": "size",
        "Description": "description",
    }
    df = pd.read_html(txt)[0]
    df = df.dropna(subset="Last modified")
    df = df[list(col_rename)].rename(columns=col_rename)

    if not len(df):
        return df

    df["dataset_code"] = dataset_code
    df["dataset"] = dataset
    df["url"] = f"{dataset_code}/{month}/" + df["file_name"]
    # if in the current month, the files will not be gzipped and will
    # have a .txt extension instead of .txt.gz
    file_extension = df["file_name"].str.split(".").str[-1].iloc[0]

    if file_extension == "txt":
        df["station_id"] = df["file_name"].str.split(".").str[0]
        df["txt_url"] = utils.BASE_URL + "/" + df["url"]

    else:
        # they put year at the end: 4103712022.txt.gz
        df["station_id"] = df["file_name"].str.split(".").str[0].str[:-5]
        df["txt_url"] = df.apply(
            lambda row: _build_txt_url(
                row["file_name"], dataset_code, month
            ),
            axis=1,
        )
    df["timeframe"] = "current_year"
    return df


def parse_avail_current_year(data, dataset):

    df_store = []
    for month, txt in data.items():

        df = parse_avail_current_year_month(txt, dataset, month)

        df_store.append(df)

    return pd.concat(df_store)

# MAIN INTERFACE

def avail_current_year(dataset="standard"):

    if dataset == "all":
        datasets = list(DATASETS)
    else:
        datasets = [dataset]
    
    df_store = []
    for ds in datasets:
        data = extract_avail_current_year(ds)
        df = parse_avail_current_year(data, ds)
        df_store.append(df)
    
    return pd.concat(df_store)


def get_dataset(txt_url, dataset, rename_cols=True):

    txt = utils.get_url(txt_url)

    if dataset == "standard":
        df = historical.parse_standard(txt, rename_cols=rename_cols)

    elif dataset == "oceanographic":
        df = historical.parse_oceanographic(txt, rename_cols=rename_cols)

    elif dataset == "supplemental":
        df = historical.parse_supplemental(txt)

    elif dataset == "raw_spectral":
        df = historical.parse_raw_spectral(txt)

    elif dataset == "spectral_summary":
        df = historical.parse_spectral_summary(txt)

    elif dataset == "spectral_alpha1":
        df = historical.parse_spectral_alpha1(txt)

    elif dataset == "spectral_alpha2":
        df = historical.parse_spectral_alpha2(txt)

    elif dataset == "spectral_r1":
        df = historical.parse_spectral_r1(txt)

    elif dataset == "spectral_r2":
        df = historical.parse_spectral_r2(txt)
    else:
        raise ValueError(f"Dataset must be one of {list(DATASETS)}.")
    
    return df