import requests
from io import StringIO
import pandas as pd
from bs4 import BeautifulSoup
from .request import _make_request

BASE_URL = "http://www.ndbc.noaa.gov/view_text_file.php?filename="


def _get_all_urls(buoy):
    """Scrape all the urls for the given buoy. Parses urls at:

    https://www.ndbc.noaa.gov/station_history.php?station=41037

    and finds all the urls that refer to .txt files.
    """

    url = f"https://www.ndbc.noaa.gov/station_history.php?station={buoy}"

    txt = _make_request(url)

    soup = BeautifulSoup(txt)

    urls = soup.find("ul").find_all("a", href=True)
    data_urls = []
    for u in urls:
        if ".txt" in u["href"]:
            data_urls.append(u["href"])

    return data_urls


def available_downloads(buoy):
    """Parse out what the urls actually denote. Date and data type.

    Link examples:
    ex1: /download_data.php?filename=41037h2016.txt.gz&dir=data/historical/stdmet/
    ex2: /download_data.php?filename=4103772020.txt.gz&dir=data/stdmet/Jul/
    ex3: /data/ocean/Aug/41037.txt
    """

    data_urls = _get_all_urls(buoy)
    data_dict = []

    for link in data_urls:
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
            year = max([x["year"] for x in data_dict])
            month = link.split("/")[3]

        data_dict.append(
            {"year": year, "month": month, "dataset": dataset, "url": link}
        )
    df = pd.DataFrame(data_dict)

    # Those without a month are assigned Jan for datetime parsing
    m = df["month"] == ""
    df.loc[m, "month"] = "Jan"
    df["date"] = pd.to_datetime(df[["year", "month"]].sum(axis=1), format="%Y%b")

    return df.sort_values(["dataset", "date"])


def ndbc_historic(buoy, year, dataset="stdmet"):
    """Get historical data.

    Parameters
    ----------
    year: int or list of ints
        Years to pull data. Can either be 2008 or [2008, 2009]
    """

    df_avail = available_downloads(buoy)

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

        qs = url.split("?")[-1]
        data_url = f"https://www.ndbc.noaa.gov/view_text_file.php?{qs}"
        txt = _make_request(data_url)

        df = parsers[dataset](txt)
        df_store.append(df)

    return pd.concat(df_store)


def _stdmet(txt):

    df = pd.read_csv(
        StringIO(txt),
        header=0,
        delim_whitespace=True,
        na_values=[99, 999, 9999, 99.0, 999.0, 9999.0],
    )

    print(df.head(3))
    # first row is units, so drop it. data after 2007 has units
    if df.iloc[0, 0] == "#yr":
        df = df.drop(df.index[0])

    # data before 2007 does not always have minute
    if "mm" in df.columns:
        res = df.iloc[:, :5].astype(str).agg("-".join, axis=1)
        df["date"] = pd.to_datetime(res, format="%Y-%m-%d-%H-%M")

    else:
        res = df.iloc[:, :4].agg("-".join, axis=1)
        df["date"] = pd.to_datetime(res, format="%Y-%m-%d-%H")

    df = df.set_index("date")

    # cols = [
    #     "WDIR",
    #     "WSPD",
    #     "GST",
    #     "WVHT",
    #     "DPD",
    #     "APD",
    #     "MWD",
    #     "PRES",
    #     "ATMP",
    #     "WTMP",
    #     "DEWP",
    #     "VIS",
    #     "TIDE",
    # ]
    # df = df[cols].astype(float)
    df.columns = df.columns.str.lower()
    return df.astype(float)
