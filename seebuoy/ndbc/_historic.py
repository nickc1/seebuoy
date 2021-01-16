import requests
from io import StringIO
import pandas as pd
from bs4 import BeautifulSoup
from ._request import make_request

BASE_URL = "http://www.ndbc.noaa.gov/view_text_file.php?filename="


def _get_all_urls(buoy):
    """Scrape all the urls for the given buoy. Parses urls at:

    https://www.ndbc.noaa.gov/station_history.php?station=41037

    and finds all the urls that refer to .txt files.
    """

    url = f"https://www.ndbc.noaa.gov/station_history.php?station={buoy}"

    txt = make_request(url)

    soup = BeautifulSoup(txt, features="html.parser")

    urls = soup.find("ul").find_all("a", href=True)
    data_urls = []
    for u in urls:
        if ".txt" in u["href"]:
            data_urls.append(u["href"])

    return data_urls


def _parse_urls(data_urls):
    """parses the urls returned from _get_all_urls"""

    data = []
    for link in data_urls:
        # some buoys have a climatic summary. For now, we ignore this.
        if "climatic" in link:
            continue

        # remove /historical so they are all the same
        dataset = link.replace("/historical", "").split("data/")[-1].split("/")[0]

        if "filename" in link:
            year = link.split("filename=")[-1].split(".txt")[0][-4:]

            if "historical" not in link:
                month = link.split("/")[-2]
            else:
                month = ""

        else:
            # get max year from previously passed dates.
            year = max([x["year"] for x in data])
            month = link.split("/")[3]

        data.append({"year": year, "month": month, "dataset": dataset, "url": link})
    return data


def available_datasets(buoy):
    """Parse out what the urls actually denote. Date and data type.

    Link examples:

    - ex1: /download_data.php?filename=41037h2016.txt.gz&dir=data/historical/stdmet/
    - ex2: /download_data.php?filename=4103772020.txt.gz&dir=data/stdmet/Jul/
    - ex3: /data/ocean/Aug/41037.txt

    Args:
        buoy (int): buoy id

    Returns:
        Dataframe of the available downloads
    """

    data_urls = _get_all_urls(buoy)
    data = _parse_urls(data_urls)
    df = pd.DataFrame(data)

    # Those without a month are assigned Jan for datetime parsing
    m = df["month"] == ""
    df.loc[m, "month"] = "Jan"
    df["date"] = pd.to_datetime(df[["year", "month"]].sum(axis=1), format="%Y%b")

    return df.sort_values(["dataset", "date"])


def historic(buoy, year, dataset="stdmet"):
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

        txt = make_request(data_url)

        df = parsers[dataset](txt)
        df_store.append(df)

    return pd.concat(df_store)


def _stdmet(txt):

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


def available_years(buoy, dataset):
    df = available_datasets(buoy)
    mask = df["dataset"] == dataset
    years = df.loc[mask, "year"].unique()
    return years


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
