from io import StringIO
import pandas as pd

def _build_txt_url(name, suffix):
    base_url = "https://www.ndbc.noaa.gov/view_text_file.php?filename"
    url = f"{base_url}={name}&dir=data/historical/{suffix}/"
    
    return url

def parse_avail_historical(txt, dataset=None):

    df_raw = pd.read_html(txt)[0]

    df = df_raw.dropna(subset=["Last modified"])
    col_rename = {
        "Name": "name",
        "Last modified": "last_modified",
        "Size": "size",
        "Description": "description"
    }
    df = df[list(col_rename)].rename(columns=col_rename)    

    # Example file name: 42007h1989.txt.gz
    df["compression"] = df["name"].str.split('.').str[-1]
    df["file_extension"] = df["name"].str.split('.').str[-2]
    df["file_name"] = df["name"].str.split('.').str[0]
    df["buoy_id"] = df["file_name"].str[:-5]
    df["file_year"] = df["file_name"].str[-4:]

    # https://www.ndbc.noaa.gov/view_text_file.php?filename=41037h2005.txt.gz&dir=data/historical/stdmet/
    df["txt_url"] = df.apply(lambda row: _build_txt_url(row['name'], row['suffix']), axis=1)

    if dataset is not None:
        df["dataset"] = dataset

    return df


def parse_all_avail_historical(data):

    df_store = []
    for dataset, txt in data.items():

        df = parse_avail_historical(txt, dataset)
        df_store.append(df)

    return pd.concat(df_store)


def standard(txt):
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

    df.columns = df.columns.str.lower()
    return df.astype(float)


# DEPRECATED
def historic_data(buoy, year, dataset="stdmet"):
    """Retrieves historical data for the requested buoy, year, and dataset.
    Need to cover both urls:

    https://www.ndbc.noaa.gov/data/stdmet/Nov/41108.txt
    https://www.ndbc.noaa.gov/view_text_file.php?filename=41108a2020.txt.gz&dir=data/stdmet/Oct/

    Args:
        buoy (int): Buoy Id
        year (int): Years to pull data.
        dataset (str): Which dataset to pull in. To see list of available
            datasets, see ndbc.available_downloads
    """

    df_avail = available_datasets(buoy)

    parsers = {
        "stdmet": _stdmet,
    }

    if dataset not in df_avail["dataset"].values:
        raise ValueError(
            f"Dataset {dataset} not available for year {year} on buoy {buoy}"
        )

    if dataset not in parsers:
        raise ValueError("Dataset must be one of {}".format(", ".join(parsers)))

    m1 = df_avail["dataset"] == dataset
    m2 = df_avail["year"] == str(year)
    df_store = []
    for url in df_avail.loc[m1 & m2, "url"].values:

        if "?" in url:
            qs = url.split("?")[-1]
            data_url = f"https://www.ndbc.noaa.gov/view_text_file.php?{qs}"
        else:
            data_url = f"https://www.ndbc.noaa.gov{url}"

        txt = get_url(data_url)

        df = parsers[dataset](txt)
        df_store.append(df)

    return pd.concat(df_store)



def all_historic(buoy, dataset="stdmet"):
    """Retrieve all historic data for a given buoy and dataset.
    This can pull a significant amount of data, so be kind to NDBC.

    Args:
        buoy (int): Buoy Id
        dataset (str): Which dataset to pull in. To see list of available
            datasets, see ndbc.available_downloads
    """

    years = available_years(buoy, dataset)

    if not len(years):
        raise ValueError(
            f"No {dataset} data available for buoy {buoy}."
            "Try running `ndbc.available_datasets(buoy, dataset)` to see"
            "available data"
        )

    df_store = []
    for year in years:
        print(f"Pulling {dataset} for {year}")
        df = historic(buoy, year, dataset)
        df_store.append(df)

    df = pd.concat(df_store)

    return df
