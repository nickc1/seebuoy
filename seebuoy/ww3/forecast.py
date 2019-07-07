import pandas as pd
from datetime import datetime
import numpy as np
import netCDF4


class WW3:
    """Pull wave watch 3 model data in Pandas dataframes.
    
    Parameters
    ----------
    date : str
        Date of data to pull. If None, it will return the most recent
    
    """
    def __init__(self,date=None):
        
        if date is None:
            date = datetime.today().strftime("%Y%m%d")
        
        base_url = "https://nomads.ncep.noaa.gov:9090/dods/wave/nww3/nww3"
        self.url = "{}{}/nww3{}_00z".format(base_url, date, date)
    
    def get_file(self):
        """Get the netcdf file constructed from the URL. By default all
        variables are kept.
        """

        self.dataset = netCDF4.Dataset(self.url)
    
    def get_var(self, var):
        """Get the specified variable from the netcdf file.
         
        dirpwsfc - primary wave direction [deg]
    
        dirswsfc - secondary wave direction [deg]
    
        htsgwsfc- significant height of combined wind waves and swell [m]
    
        perpwsfc - primary wave mean period [s]
    
        perswsfc - secondary wave mean period [s]
    
        ugrdsfc - u-component of wind [m/s]
    
        vgrdsfc - v-component of wind [m/s]
    
        wdirsfc - wind direction (from which blowing) [deg]
    
        windsfc - wind speed [m/s]
    
        wvdirsfc - direction of wind waves [deg]
    
        wvpersfc - mean period of wind waves [s] 
        Parameters
        ----------
        var : str
            Variable from the netcdf file to retreive.
        
        Returns
        -------
        x : np.array
            Returns the requested data assiciated with the variable.
        """

        return self.dataset.variables[var].data
    
    def get_closest_lat_lon(self, lat, lon):
        """Retrieves the indexes of the closest latitude and longitude.
        
        Parameters
        ----------
        lat : float
            Latitude to find closest of.
        lon : float
            Longitude to find the closest.
        """

        
        if lon < 0:
            lon = 360 + lon
        
        xlats = self.dataset.variables['lat'][:].data
        xlons = self.dataset.variables['lon'][:].data
        

        lat_idx = np.abs(lat - xlats).argmin()
        lon_idx = np.abs(lon - xlons).argmin()

        return lat_idx, lon_idx


    
    def get_lat_lon_var(self, lat, lon, var):
        """Get the requested variable at the requested latitude and longitude.
        Latitudes range from -80 to 80 and longitudes range from 0 to 360.
        If a negative longitude is fed, it will be converted to a 0 to 360
        range.

        Parameters
        ----------
        lat : float
            Latitude of variable.
        lon : float
            Longitude of variable.
        var : str
            Varibale to retrieve.
        """

        lat_idx, lon_idx = self.get_closest_lat_lon(lat, lon)

        return self.dataset.variables[var].data[1:, lat_idx, lon_idx]
    
    def get_standard_df(self, lat, lon):
        """Gets the standard data for a specific latitude and longitude.
        Returns it as a dataframe.

        Parameters
        ----------
        lat : float
            Latitude to retrieve for the data
        lon : float
            Longitude to retrieve for the data
        
        Returns
        -------
        df : pd.DataFrame
            Dataframe of standard values
        """


        today = datetime.today().strftime('%Y-%m-%d')
        dti = pd.date_range(today, periods=60, freq='3H')

        lat_idx, lon_idx = self.get_closest_lat_lon(lat, lon)

        dvars = ['dirpwsfc', 'dirswsfc', 'htsgwsfc', 'perpwsfc', 'perswsfc', 'ugrdsfc',
                'vgrdsfc', 'wdirsfc', 'windsfc', 'wvdirsfc', 'wvpersfc']
        
        df = {}
        for var in dvars:
            print(var)
            df[var] = self.dataset.variables[var][1:, lat_idx, lon_idx]

        
        df = pd.DataFrame(df, index=dti)

        return df





