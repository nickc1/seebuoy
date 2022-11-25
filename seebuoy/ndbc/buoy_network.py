
import pandas as pd
from . import extract
from .transform import recent as tr_recent
from .transform import metadata as tr_metadata
from .transform import historic as tr_historic
from .transform import current_year as tr_current

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

        self.df_recent_data = df

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
        