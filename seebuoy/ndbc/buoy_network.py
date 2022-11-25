
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

    def available_recent_data(self):

        txt = extract.avail_recent_datasets()
        df = tr_recent.parse_avail_recent_datasets(txt)

        self.df_recent_data = df

        return df
    
    def available_current_year(self):

        for dataset in extract.HIST_DATASETS:
            txt = extract.avail_current_year_months(dataset)
            df_months = tr_

    def available_historic_data(self):

        data = extract.all_avail_historical()
        df = tr_historic.parse_all_avail_historical(data)

        self.df_historic = df

        return df
        