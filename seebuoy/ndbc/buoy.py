import requests
import pandas as pd
from .transform import recent, historic, current_year, parser
from .buoy_network import BuoyNetwork

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


class Buoy(BuoyNetwork):
    def __init__(self, station_id, rename_cols=True):

        self.station_id = station_id
        self.rename_cols = rename_cols

    def get_available_data(self, dataset="standard"):

        df = self.available_data(dataset=dataset)
        m = df["station_id"] == self.station_id

        df = df[m].copy()

        self.df_avail = df

        return df

    def get_data(
        self,
        dataset="standard",
        data_group="all",
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

        df_store = []
        for row in self.df_avail.to_dict(orient="records"):

            data_group = row["data_group"]
            url = row["txt_url"]

            txt = get_url(url)

            if txt is None:
                df = pd.DataFrame()

            if dataset == "standard":
                df = parser.standard(txt, data_group, rename_cols=self.rename_cols)

            elif dataset == "oceanographic":
                df = parser.oceanographic(txt, data_group, rename_cols=self.rename_cols)

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
