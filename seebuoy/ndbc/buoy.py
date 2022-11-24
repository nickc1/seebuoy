
import requests
import pandas as pd
from . import recent
from . import historic

DATASET_MAP ={
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


class Buoy:

    def __init__(self, buoy_id, rename_cols=True):

        self.buoy_id = buoy_id
        self.rename_cols = rename_cols
        self.recent_url = "http://www.ndbc.noaa.gov/data/realtime2"
        self.df_available = historic.available_datasets(buoy_id)
    
    
    def get_recent_data(self, dataset="standard"):
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
        
        data_suffix = DATASET_MAP[dataset]

        url = f"{self.recent_url}/{self.buoy_id}.{data_suffix}"
        
        txt = get_url(url)

        if txt is None:
            return None

        if dataset == "standard":
            df = recent.standard(txt, rename_cols=self.rename_cols)
        
        elif dataset == "oceanographic":
            df = recent.oceanographic(txt, rename_cols=self.rename_cols)
        
        elif dataset == "supplemental":
            df = recent.supplemental(txt)

        elif dataset == "raw_spectral":
            df = recent.raw_spectral(txt)
        
        elif dataset == "spectral_summary":
            df = recent.spectral_summary(txt)        

        elif dataset == "spectral_alpha1":
            df = recent.spectral_alpha1(txt)
        
        elif dataset == "spectral_alpha2":
            df = recent.spectral_alpha2(txt)

        elif dataset == "spectral_r1":
            df = recent.spectral_r1(txt)
        
        elif dataset == "spectral_r2":
            df = recent.spectral_r2(txt)
        else:
            raise ValueError(f"Dataset must be one of {list(DATASET_MAP)}.")           
        
        df.columns = df.columns.str.lower()

        return df


    def get_historic_data(self, years, dataset="standard"):
        """Get the historical data for the given buoy.

            - standard: Standard Meteorological Data. [stdmet]
        
            Args:
                years (list): List of years to pull the historical data.
                dataset (str): What dataset to query. Possible values are:
                    'standard'.

            Returns:
                DataFrame containing the requested data.
        """

        mask = self.df_avail["dataset"] == dataset
        df_avail = self.df_avail[mask].copy()

        if years == "all":
            pull_years = df_avail["year"].unique()

            if not len(years):
                raise ValueError(
                    f"No {dataset} data available for buoy {self.buoy_id}."
                    "Check the df_avail attribute to see the list of available datasets."
                )
        else:
            pull_years = [yr for yr in years if yr in df_avail["year"].unique()]

            if len(years) != len(pull_years):
                print(f"Not all years are available. Pulling: {pull_years}")

        # get the raw text data
        df_store = []
        for year in pull_years:
            m = df_avail["year"] == year
            url = df_avail.loc[m, url]
            
            # slight adjustments needed to url
            if "?" in url:
                qs = url.split("?")[-1]
                data_url = f"https://www.ndbc.noaa.gov/view_text_file.php?{qs}"
            else:
                data_url = f"https://www.ndbc.noaa.gov{url}"

            txt = get_url(data_url)

            if dataset == "standard":
                df = historic.standard(txt)
            else:
                raise ValueError("Only standard implemented right now.")
            
            df_store.append(df)

        return pd.concat(df)

        