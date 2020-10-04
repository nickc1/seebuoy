import requests
from io import StringIO
import pandas as pd
from bs4 import BeautifulSoup

BASE_URL = "http://www.ndbc.noaa.gov/view_text_file.php?filename="


def _get_all_urls(buoy):
    """Scrape all the urls for the given buoy. Parses urls at:
    
    https://www.ndbc.noaa.gov/station_history.php?station=41037

    and finds all the urls that refer to .txt files.
    """

    url = f"https://www.ndbc.noaa.gov/station_history.php?station={buoy}"

    resp = requests.get(url)
    print(resp.text)
    soup = BeautifulSoup(resp.text)

    uls = soup.find_all("ul")
    data_links = []
    for u in uls:
        for link in u.find_all("a", href=True):
            if ".txt" in link["href"]:
                data_links.append(link["href"])

    return data_links


def available_downloads(data_links):
    """Parse out what the urls actually denote. Date and data type.

    Link examples:
    ex1: /download_data.php?filename=41037h2016.txt.gz&dir=data/historical/stdmet/
    ex2: /download_data.php?filename=4103772020.txt.gz&dir=data/stdmet/Jul/
    ex3: /data/ocean/Aug/41037.txt
    """
    data_dict = []

    for link in data_links:
        # remove /historical so they are all the same
        data_type = link.replace("/historical", "").split("data/")[-1].split("/")[0]

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
            {"year": year, "month": month, "data_type": data_type, "url": link}
        )
    df = pd.DataFrame(data_dict)

    # Those without a month are assigned Jan for datetime parsing
    m = df["month"] == ""
    df.loc[m, "month"] = "Jan"
    df["date"] = pd.to_datetime(df[["year", "month"]].sum(axis=1), format="%Y%b")

    return df.sort_values(['data_type', 'date'])


def ndbc_historic(buoy, year, dataset="stdmet"):
    """Get historical data.

    Parameters
    ----------
    year: int or list of ints
        Years to pull data. Can either be 2008 or [2008, 2009]
    """

    url = "{BASE_URL}{buoy}h{year}.txt.gz&dir=data/historical/{dataset}/"

    opts = {
        "stdmet": _stand_meteo,
    }

    if dataset not in opts:
        raise ValueError("Dataset must be one of {}".format(", ".join(opts)))

    if type(year) != list:
        year = list(year)

    for yr in year:
        url = "{BASE_URL}{buoy}h{yr}.txt.gz&dir=data/historical/{dataset}/"
        txt = _make_request(url)
        opts["stdmet"]

    if txt is None:
        return

    return opts[dataset](txt)


def _stdmet(txt):

    df = pd.read_csv(
        StringIO(txt),
        header=0,
        delim_whitespace=True,
        na_values=[99, 999, 9999, 99.0, 999.0, 9999.0],
    )

    # first row is units, so drop it. data after 2007 has units
    if df.iloc[0, 0] == "#yr":
        df = df.drop(df.index[0])

    # data before 2007 does not always have minute
    if "mm" in df.columns:
        df["date"] = pd.to_datetime(df["YY", "MM", "DD", "hh", "mm"])
    else:
        df["date"] = pd.to_datetime(df["YY", "MM", "DD", "hh"])
    df = df.set_index("date")

    cols = [
        "WDIR",
        "WSPD",
        "GST",
        "WVHT",
        "DPD",
        "APD",
        "MWD",
        "PRES",
        "ATMP",
        "WTMP",
        "DEWP",
        "VIS",
        "TIDE",
    ]
    df = df[cols].astype(float)

    return df


class Historic:
    def __init__(self, buoy, year, year_range=None):

        link = "http://www.ndbc.noaa.gov/view_text_file.php?filename="
        link += "{}h{}.txt.gz&dir=data/historical/".format(buoy, year)
        self.link = link

    def get_stand_meteo(self, link=None):
        """
        Standard Meteorological Data. Data header was changed in 2007. Thus
        the need for the if statement below.



        WDIR    Wind direction (degrees clockwise from true N)
        WSPD    Wind speed (m/s) averaged over an eight-minute period
        GST     Peak 5 or 8 second gust speed (m/s)
        WVHT    Significant wave height (meters) is calculated as
                the average of the highest one-third of all of the
                wave heights during the 20-minute sampling period.
        DPD     Dominant wave period (seconds) is the period with the maximum wave energy.
        APD     Average wave period (seconds) of all waves during the 20-minute period.
        MWD     The direction from which the waves at the dominant period (DPD) are coming.
                (degrees clockwise from true N)
        PRES    Sea level pressure (hPa).
        ATMP    Air temperature (Celsius).
        WTMP    Sea surface temperature (Celsius).
        DEWP    Dewpoint temperature
        VIS     Station visibility (nautical miles).
        PTDY    Pressure Tendency
        TIDE    The water level in feet above or below Mean Lower Low Water (MLLW).
        """

        link = self.link + "stdmet/"

        # combine the first five date columns YY MM DD hh and make index
        df = pd.read_csv(
            link,
            header=0,
            delim_whitespace=True,
            dtype=object,
            na_values=[99, 999, 9999, 99.0, 999.0, 9999.0],
        )

        # 2007 and on format
        if df.iloc[0, 0] == "#yr":

            df = df.rename(columns={"#YY": "YY"})  # get rid of hash

            # make the indices

            df.drop(0, inplace=True)  # first row is units, so drop them

            d = df.YY + " " + df.MM + " " + df.DD + " " + df.hh + " " + df.mm
            ind = pd.to_datetime(d, format="%Y %m %d %H %M")

            df.index = ind

            # drop useless columns and rename the ones we want
            df.drop(["YY", "MM", "DD", "hh", "mm"], axis=1, inplace=True)
            df.columns = [
                "WDIR",
                "WSPD",
                "GST",
                "WVHT",
                "DPD",
                "APD",
                "MWD",
                "PRES",
                "ATMP",
                "WTMP",
                "DEWP",
                "VIS",
                "TIDE",
            ]

        # before 2006 to 2000
        else:
            date_str = df.YYYY + " " + df.MM + " " + df.DD + " " + df.hh

            ind = pd.to_datetime(date_str, format="%Y %m %d %H")

            df.index = ind

            # some data has a minute column. Some doesn't.

            if "mm" in df.columns:
                df.drop(["YYYY", "MM", "DD", "hh", "mm"], axis=1, inplace=True)
            else:
                df.drop(["YYYY", "MM", "DD", "hh"], axis=1, inplace=True)

            df.columns = [
                "WDIR",
                "WSPD",
                "GST",
                "WVHT",
                "DPD",
                "APD",
                "MWD",
                "PRES",
                "ATMP",
                "WTMP",
                "DEWP",
                "VIS",
                "TIDE",
            ]

        # all data should be floats
        df = df.astype("float")

        return df

    def get_all_stand_meteo(self):
        """
        Retrieves all the standard meterological data. Calls get_stand_meteo.
        It also checks to make sure that the years that were requested are
        available. Data is not available for the same years at all the buoys.

        Returns
        -------
        df : pandas dataframe
            Contains all the data from all the years that were specified
            in year_range.
        """

        start, stop = self.year_range

        # see what is on the NDBC so we only pull the years that are available
        links = []
        for ii in range(start, stop + 1):

            base = "http://www.ndbc.noaa.gov/view_text_file.php?filename="
            end = ".txt.gz&dir=data/historical/stdmet/"
            link = base + str(self.buoy) + "h" + str(ii) + end

            try:
                urllib2.urlopen(link)
                links.append(link)

            except IndexError:
                print(str(ii) + " not in records")

        # need to also retrieve jan, feb, march, etc.
        month = [
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ]
        k = [1, 2, 3, 4, 5, 6, 7, 8, 9, "a", "b", "c"]  # for the links

        for ii in range(len(month)):
            mid = ".txt.gz&dir=data/stdmet/"
            link = (
                base + str(self.buoy) + str(k[ii]) + "2016" + mid + str(month[ii]) + "/"
            )

            try:
                urllib2.urlopen(link)
                links.append(link)

            except IndexError:
                print(str(month[ii]) + "2016" + " not in records")
                print(link)

        # start grabbing some data
        df = pd.DataFrame()  # initialize empty df

        for L in links:

            new_df = self.get_stand_meteo(link=L)
            print("Link : " + L)
            df = df.append(new_df)

        return df
