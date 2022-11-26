import requests
import pandas as pd
from . import extract
from .transform import recent as tr_recent
from .transform import metadata as tr_metadata
from .transform import historic as tr_historic
from .transform import current_year as tr_current
from .transform import parser


DATASET_MAP = {
    "standard": "txt",
    "oceanographic": "ocean",
    "supplemental": "supl",
    "raw_spectral": "data_spec",
    "spectral_summary": "spec",
    "spectral_alpha1": "swdir",
    "spectral_alpha2": "swdir2",
    "spectral_r1": "swr1",
    "spectral_r2": "swr2",
}


def get_url(url):

    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.text
    elif resp.status_code == 404:
        print(f"Dataset not available (404 Error) for url: \n {url}")
        return None
    else:
        raise ValueError(f"Error code {resp.status_code} for url: \n {url}")


class BuoyNetwork:
    def __init__(self):
        pass

    def buoys(self, add_closest_cities=True, add_owners=True):

        txt = extract.buoy_locations()
        df = tr_metadata.parse_buoy_locations(txt)

        if add_closest_cities:
            df = tr_metadata.add_closest_cities(df)

        if add_owners:
            txt = extract.buoy_owners()
            df_owners = tr_metadata.parse_buoy_owners(txt)
            df = tr_metadata.add_owners(df, df_owners)

        self.df_buoys = df

        return df

    def available_recent_data(self, dataset="standard"):

        if dataset == "all":
            dataset = list(extract.HIST_DATASETS)
        elif isinstance(dataset, str):
            dataset = [dataset]

        txt = extract.avail_recent_datasets()
        df = tr_recent.parse_avail_recent_datasets(txt)

        # subset out dataset
        m = df["dataset"].isin(dataset)
        df = df[m].copy()

        self.df_recent = df

        return df

    def available_current_year_data(self, dataset="standard"):

        if dataset == "all":
            dataset = list(extract.HIST_DATASETS)
        elif isinstance(dataset, str):
            dataset = [dataset]

        df_store = []
        for dset in dataset:
            data = extract.avail_current_year(dset)
            df = tr_current.parse_avail_current_year(data, dset)

            df_store.append(df)

        df = pd.concat(df_store)

        self.df_current_yr = df

        return df

    def available_historic_data(self, dataset="standard"):

        if dataset == "all":
            dataset = list(extract.HIST_DATASETS)
        elif isinstance(dataset, str):
            dataset = [dataset]

        df_store = []
        for d in dataset:
            txt = extract.avail_historical(d)

            df = tr_historic.parse_avail_historical(txt, dataset=d)
            df_store.append(df)

        df = pd.concat(df_store)
        self.df_historic = df

        return df

    def available_data(self, dataset="standard"):

        df_recent = self.available_recent_data(dataset)

        df_current_yr = self.available_current_year_data(dataset)

        df_historic = self.available_historic_data(dataset)

        df_recent["data_group"] = "recent"
        df_current_yr["data_group"] = "current_year"
        df_historic["data_group"] = "historic"

        df = pd.concat([df_recent, df_current_yr, df_historic])

        self.df_avail = df

        return df

    def available_buoy_data(self, station_id):

        m = self.df_avail["station_id"] == station_id
        df = self.df_avail[m].copy()

        return df

    def get_data(
        self,
        station_id,
        dataset="standard",
        data_group="all",
        rename_cols=True,
        drop_duplicates=True,
        start_date=None,
        end_date=None,
    ):
        """Get recent data from the NDBC. Most buoys have six different data sources
        to pull from:

            - standard: Standard Meteorological Data. [txt]
            - oceanographic: Oceanographic Data [ocean]
            - supplemental: Supplemental Measurements Data [supl]
            - raw_spectral: Raw Spectral Wave Data. [data_spec]
            - spectral_summary: Spectral Wave Summary Data [spec]
            - spectral_alpha1: Spectral Wave Data (alpha1) [swdir]
            - spectral_alpha2: Spectral Wave Data (alpha2) [swdir2]
            - spectral_r1: Spectral Wave Data (r1) [swr1]
            - spectral_r2: Spectral Wave Data (r2) [swr2]


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

        m1 = self.df_avail["station_id"] == station_id
        m2 = self.df_avail["dataset"] == dataset
        df_avail = self.df_avail[m1 & m2]

        df_store = []

        for row in df_avail.to_dict(orient="records"):

            data_group = row["data_group"]
            url = row["txt_url"]

            txt = get_url(url)

            if txt is None:
                df = pd.DataFrame()

            if dataset == "standard":
                df = parser.standard(txt, data_group, rename_cols=rename_cols)

            elif dataset == "oceanographic":
                df = parser.oceanographic(txt, data_group, rename_cols=rename_cols)

            elif dataset == "supplemental":
                df = parser.supplemental(txt, data_group)

            elif dataset == "raw_spectral":
                df = parser.raw_spectral(txt, data_group)

            elif dataset == "spectral_summary":
                df = parser.spectral_summary(txt, data_group)

            elif dataset == "spectral_alpha1":
                df = parser.spectral_alpha1(txt, data_group)

            elif dataset == "spectral_alpha2":
                df = parser.spectral_alpha2(txt, data_group)

            elif dataset == "spectral_r1":
                df = parser.spectral_r1(txt, data_group)

            elif dataset == "spectral_r2":
                df = parser.spectral_r2(txt, data_group)
            else:
                raise ValueError(f"Dataset must be one of {list(DATASET_MAP)}.")

            df["url"] = row["url"]
            df["txt_url"] = url
            df_store.append(df)
        df = pd.concat(df_store)

        if drop_duplicates:
            df = df[~df.index.duplicated(keep="first")]

        return df.sort_index()
