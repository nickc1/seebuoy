
import pandas as pd
from . import metadata
from . import real_time
from . import current_year
from . import historical
from . import utils


class NDBC:
    """Interface to the data available from the NDBC. The available datasets
    supported are:

    adcp: Acoustic Doppler Current Profiler Current Year Historical Data [adcp]
    adcp2: Acoustic Doppler Current Profiler Current Year Historical Data [adcp2]
    continuous_wind: Continuous Winds Current Year Historical Data [cwind]
    water_col_height: Water Column Height (DART) Current Year Historical Data [dart]
    mmbcur: No description available [mmbcur]
    oceanographic: Oceanographic Current Year Historical Data [ocean]
    rain_hourly: Hourly Rain Current Year Historical Data [rain]
    rain_10_min: 10 Minute Rain Current Year Historical Data [rain10]
    rain_24_hr: 24 Hour Rain Current Year Historical Data [rain24]
    solar_radiation: Solar Radiation Current Year Historical Data [srad]
    standard: Standard Meteorological Current Year Historical Data [stdmet]
    supplemental: Supplemental Measurements Current Year Historical Data [supl]
    raw_spectral: Raw Spectral Wave Current Year Historical Data [swden]
    spectral_alpha1: Spectral Wave Current Year Historical Data (alpha1) [swdir]
    spectral_alpha2: Spectral Wave Current Year Historical Data (alpha2) [swdir2]
    spectral_r1: Spectral Wave Current Year Historical Data (r1) [swr1]
    spectral_r2: Spectral Wave Current Year Historical Data (r2) [swr2]
    tide: Tide Current Year Historical Data [wlevel]

    """

    def __init__(self, timeframe="all"):

        self.timeframe = timeframe


    def stations(self, station_id=None, closest_cities=True, owners=True):

        df = metadata.buoy_info(closest_cities=closest_cities, owners=owners)

        self.df_buoys = df

        if station_id:
            m = df["station_id"] == station_id
            df = df[m]
        
        return df


    def available_data(self, dataset="standard"):

        if self.timeframe == "all":
            df_real = real_time.avail_real_time(dataset)
            df_current = current_year.avail_current_year(dataset)
            df_historic = historical.avail_historical(dataset)

            df = pd.concat([df_real, df_current, df_historic])
        
        elif self.timeframe == "real_time":
            df = real_time.avail_real_time(dataset)
        
        elif self.timeframe == "current_year":
            df = current_year.avail_current_year(dataset)
        
        elif self.timeframe == 'historical':
            df = historical.avail_historical(dataset)

        else:
            raise ValueError("self.timeframe must be 'all', 'real_time', 'current_year', or 'historical'")
        
        self.df_avail = df

        return df


        return df

    def get_station(
        self,
        station_id,
        dataset="standard",
        rename_cols=True,
        drop_duplicates=True,
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
