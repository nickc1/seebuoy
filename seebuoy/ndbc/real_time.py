"""
By Nick Cortale
nickc1.github.io

Functions to query the NDBC (http://www.ndbc.noaa.gov/).

The realtime data for all of their buoys can be found at:
http://www.ndbc.noaa.gov/data/realtime2/

Info about all of noaa data can be found at:
http://www.ndbc.noaa.gov/docs/ndbc_web_data_guide.pdf

What all the values mean:
http://www.ndbc.noaa.gov/measdes.shtml

Each buoy has the data:

File                    Parameters
----                    ----------
.data_spec         Raw Spectral Wave Data
.ocean             Oceanographic Data
.spec              Spectral Wave Summary Data
.supl              Supplemental Measurements Data
.swdir             Spectral Wave Data (alpha1)
.swdir2            Spectral Wave Data (alpha2)
.swr1              Spectral Wave Data (r1)
.swr2              Spectral Wave Data (r2)
.txt               Standard Meteorological Data



Example:
import buoypy as bp

# Get the last 45 days of data
rt = bp.realtime(41013) #frying pan shoals buoy
ocean_data = rt.get_ocean()  #get Oceanographic data

wave_data.head()

Out[7]:
                    WVHT    SwH SwP WWH WWP SwD WWD STEEPNESS   APD MWD
2016-02-04 17:42:00 1.6     1.3 7.1 0.9 4.5 S   S   STEEP       5.3 169
2016-02-04 16:42:00 1.7     1.5 7.7 0.9 5.0 S   S   STEEP       5.4 174
2016-02-04 15:41:00 2.0     0.0 NaN 2.0 7.1 NaN S   STEEP       5.3 174
2016-02-04 14:41:00 2.0     1.2 7.7 1.5 5.9 SSE SSE STEEP       5.5 167
2016-02-04 13:41:00 2.0     1.7 7.1 0.9 4.8 S   SSE STEEP       5.7 175



TODO:
Make functions with except statements always spit out the same
column headings.

"""

import pandas as pd
import numpy as np
import datetime


def real_time(buoy, dataset='txt'):
    """Get realtime data from the NDBC. They have six different data sources
    to pull from:

    Parameters
    ----------
    buoy : int
        Integer of the buoy identifier. Look up buoy numbers here:
        https://www.ndbc.noaa.gov/
    source: str
        What type of data to query. Possible values are:
        - data_spec: Raw Spectral Wave Data
        - ocean: Oceanographic Data
        - spec: Spectral Wave Summary Data
        - supl: Supplemental Measurements Data
        - swdir: Spectral Wave Data (alpha1)
        - swdir2: Spectral Wave Data (alpha2)
        - swr1: Spectral Wave Data (r1)
        - swr2: Spectral Wave Data (r2)
        - txt: Standard Meteorological Data

    Returns
    -------
    df : pd.DataFrame
        DataFrame containing the requested data.
    
    Notes
    -----

    The realtime data for all of their buoys can be found at:
    http://www.ndbc.noaa.gov/data/realtime2/

    Info about all of noaa data can be found at:
    http://www.ndbc.noaa.gov/docs/ndbc_web_data_guide.pdf

    What all the values mean:
    http://www.ndbc.noaa.gov/measdes.shtml


    Example:
    from seebuoy import ndbc

    # Get the last 45 days of data
    df = ndbc.real_time(41013)
    df.head()

    Out[7]:
                        WVHT    SwH SwP WWH WWP SwD WWD STEEPNESS   APD MWD
    2016-02-04 17:42:00 1.6     1.3 7.1 0.9 4.5 S   S   STEEP       5.3 169
    2016-02-04 16:42:00 1.7     1.5 7.7 0.9 5.0 S   S   STEEP       5.4 174
    2016-02-04 15:41:00 2.0     0.0 NaN 2.0 7.1 NaN S   STEEP       5.3 174
    2016-02-04 14:41:00 2.0     1.2 7.7 1.5 5.9 SSE SSE STEEP       5.5 167
    2016-02-04 13:41:00 2.0     1.7 7.1 0.9 4.8 S   SSE STEEP       5.7 175
    """
    df = pd.DataFrame()

    return df


def _make_url(buoy, dataset):
    """Create the link to the requested dataset
    buoy: int
    dataset: str
    """"
    url = 'http://www.ndbc.noaa.gov/data/realtime2/{}.{}'
    return url.format(buoy, dataset)


def _data_spec(buoy):
    """
    Get the raw spectral wave data from the buoy. The seperation
    frequency is dropped to keep the data clean.

    Parameters
    ----------
    buoy : str
        Buoy number ex: '41013' is off wilmington, nc

    Returns
    -------
    df : pd.DataFrame (date, frequency)
        data frame containing the raw spectral data. index is the date
        and the columns are each of the frequencies

    """
    
    url = _make_url(buoy, dataset)

    #combine the first five date columns YY MM DD hh mm and make index
    df = pd.read_csv(link, delim_whitespace=True, skiprows=1, header=None,
        parse_dates=[[0,1,2,3,4]], index_col=0)


    #convert the dates to datetimes
    df.index = pd.to_datetime(df.index,format="%Y %m %d %H %M")

    specs = df.iloc[:,1::2]
    freqs = df.iloc[0,2::2]

    specs.columns=freqs

    #remove the parenthesis from the column index
    specs.columns = [cname.replace('(','').replace(')','')
        for cname in specs.columns]

    return specs


class RealTime:

    def __init__(self, buoy):

        self.link = 'http://www.ndbc.noaa.gov/data/realtime2/{}'.format(buoy)

    def data_spec(self):
        """
        Get the raw spectral wave data from the buoy. The seperation
        frequency is dropped to keep the data clean.

        Parameters
        ----------
        buoy : string
            Buoy number ex: '41013' is off wilmington, nc

        Returns
        -------
        df : pandas dataframe (date, frequency)
            data frame containing the raw spectral data. index is the date
            and the columns are each of the frequencies

        """
        
        link = "{}.{}".format(self.link, 'data_spec')

        #combine the first five date columns YY MM DD hh mm and make index
        df = pd.read_csv(link, delim_whitespace=True, skiprows=1, header=None,
            parse_dates=[[0,1,2,3,4]], index_col=0)


        #convert the dates to datetimes
        df.index = pd.to_datetime(df.index,format="%Y %m %d %H %M")

        specs = df.iloc[:,1::2]
        freqs = df.iloc[0,2::2]

        specs.columns=freqs

        #remove the parenthesis from the column index
        specs.columns = [cname.replace('(','').replace(')','')
            for cname in specs.columns]

        return specs


    def ocean(self):
        """
        Retrieve oceanic data. For the buoys explored,
        O2%, O2PPM, CLCON, TURB, PH, EH were always NaNs


        Returns
        -------
        df : pandas dataframe
            Index is the date and columns are:
            DEPTH   m
            OTMP    degc
            COND    mS/cm
            SAL     PSU
            O2%     %
            02PPM   ppm
            CLCON   ug/l
            TURB    FTU
            PH      -
            EH      mv

        """

        link = "{}.{}".format(self.link, 'ocean')

        #combine the first five date columns YY MM DD hh mm and make index
        df = pd.read_csv(link, delim_whitespace=True, na_values='MM',
            parse_dates=[[0,1,2,3,4]], index_col=0)

        #units are in the second row drop them
        df.drop(df.index[0], inplace=True)

        #convert the dates to datetimes
        df.index = pd.to_datetime(df.index,format="%Y %m %d %H %M")

        #convert to floats
        cols = ['DEPTH','OTMP','COND','SAL']
        df[cols] = df[cols].astype(float)


        return df


    def spec(self):
        """
        Get the spectral wave data from the ndbc. Something is wrong with
        the data for this parameter. The columns seem to change randomly.
        Refreshing the data page will yield different column names from
        minute to minute.

        parameters
        ----------
        buoy : string
            Buoy number ex: '41013' is off wilmington, nc

        Returns
        -------
        df : pandas dataframe
            data frame containing the spectral data. index is the date
            and the columns are:

            HO, SwH, SwP, WWH, WWP, SwD, WWD, STEEPNESS, AVP, MWD

            OR

            WVHT  SwH  SwP  WWH  WWP SwD WWD  STEEPNESS  APD MWD


        """

        link = "{}.{}".format(self.link, 'spec')

        #combine the first five date columns YY MM DD hh mm and make index
        df = pd.read_csv(link, delim_whitespace=True, na_values='MM',
            parse_dates=[[0,1,2,3,4]], index_col=0)

        try:
            #units are in the second row drop them
            #df.columns = df.columns + '('+ df.iloc[0] + ')'
            df.drop(df.index[0], inplace=True)

            #convert the dates to datetimes
            df.index = pd.to_datetime(df.index,format="%Y %m %d %H %M")

            #convert to floats
            cols = ['WVHT','SwH','SwP','WWH','WWP','APD','MWD']
            df[cols] = df[cols].astype(float)
        except:

            #convert the dates to datetimes
            df.index = pd.to_datetime(df.index,format="%Y %m %d %H %M")

            #convert to floats
            cols = ['H0','SwH','SwP','WWH','WWP','AVP','MWD']
            df[cols] = df[cols].astype(float)


        return df



    def supl(self):
        """
        Get supplemental data

        Returns
        -------
        data frame containing the spectral data. index is the date
        and the columns are:

        PRES        hpa
        PTIME       hhmm
        WSPD        m/s
        WDIR        degT
        WTIME       hhmm


        """

        link = "{}.{}".format(self.link, 'supl')

        #combine the first five date columns YY MM DD hh mm and make index
        df = pd.read_csv(link, delim_whitespace=True, na_values='MM',
            parse_dates=[[0,1,2,3,4]], index_col=0)

        #units are in the second row drop them
        df.drop(df.index[0], inplace=True)

        #convert the dates to datetimes
        df.index = pd.to_datetime(df.index,format="%Y %m %d %H %M")

        #convert to floats
        cols = ['PRES','PTIME','WSPD','WDIR','WTIME']
        df[cols] = df[cols].astype(float)

        return df


    def swdir(self):
        """
        Spectral wave data for alpha 1.

        Returns
        -------

        specs : pandas dataframe
            Index is the date and the columns are the spectrum. Values in
            the table indicate how much energy is at each spectrum.
        """


        link = "{}.{}".format(self.link, 'swdir')

        #combine the first five date columns YY MM DD hh mm and make index
        df = pd.read_csv(link,delim_whitespace=True,skiprows=1,na_values=999,
            header=None, parse_dates=[[0,1,2,3,4]], index_col=0)

        #convert the dates to datetimes
        df.index = pd.to_datetime(df.index,format="%Y %m %d %H %M")

        specs = df.iloc[:,0::2]
        freqs = df.iloc[0,1::2]

        specs.columns=freqs

        #remove the parenthesis from the column index
        specs.columns = [cname.replace('(','').replace(')','')
            for cname in specs.columns]

        return specs

    def swdir2(self):
        """
        Spectral wave data for alpha 2.

        Returns
        -------

        specs : pandas dataframe
            Index is the date and the columns are the spectrum. Values in
            the table indicate how much energy is at each spectrum.
        """

        link = "{}.{}".format(self.link, 'swdir2')

        #combine the first five date columns YY MM DD hh mm and make index
        df = pd.read_csv(link,delim_whitespace=True,skiprows=1,
            header=None, parse_dates=[[0,1,2,3,4]], index_col=0)

        #convert the dates to datetimes
        df.index = pd.to_datetime(df.index,format="%Y %m %d %H %M")

        specs = df.iloc[:,0::2]
        freqs = df.iloc[0,1::2]

        specs.columns=freqs

        #remove the parenthesis from the column index
        specs.columns = [cname.replace('(','').replace(')','')
            for cname in specs.columns]

        return specs

    def swr1(self):
        """
        Spectral wave data for r1.

        Returns
        -------

        specs : pandas dataframe
            Index is the date and the columns are the spectrum. Values in
            the table indicate how much energy is at each spectrum.
        """



        link = "{}.{}".format(self.link, 'swr1')
        #combine the first five date columns YY MM DD hh mm and make index
        df = pd.read_csv(link,delim_whitespace=True,skiprows=1,
            header=None, parse_dates=[[0,1,2,3,4]], index_col=0, na_values=999)

        #convert the dates to datetimes
        df.index = pd.to_datetime(df.index,format="%Y %m %d %H %M")

        specs = df.iloc[:,0::2]
        freqs = df.iloc[0,1::2]

        specs.columns=freqs

        #remove the parenthesis from the column index
        specs.columns = [cname.replace('(','').replace(')','')
            for cname in specs.columns]

        return specs

    def swr2(self):
        """
        Spectral wave data for r2.

        Returns
        -------

        specs : pandas dataframe
            Index is the date and the columns are the spectrum. Values in
            the table indicate how much energy is at each spectrum.
        """


        link = "{}.{}".format(self.link, 'swr2')

        #combine the first five date columns YY MM DD hh mm and make index
        df = pd.read_csv(link,delim_whitespace=True,skiprows=1,
            header=None, parse_dates=[[0,1,2,3,4]], index_col=0)

        #convert the dates to datetimes
        df.index = pd.to_datetime(df.index,format="%Y %m %d %H %M")

        specs = df.iloc[:,0::2]
        freqs = df.iloc[0,1::2]

        specs.columns=freqs

        #remove the parenthesis from the column index
        specs.columns = [cname.replace('(','').replace(')','')
            for cname in specs.columns]

        return specs

    def txt(self):
        """
        Retrieve standard Meteorological data. NDBC seems to be updating
        the data with different column names, so this metric can return
        two possible data frames with different column names:

        Returns
        -------

        df : pandas dataframe
            Index is the date and the columns can be:

            ['WDIR','WSPD','GST','WVHT','DPD','APD','MWD',
            'PRES','ATMP','WTMP','DEWP','VIS','PTDY','TIDE']

            or

            ['WD','WSPD','GST','WVHT','DPD','APD','MWD','BARO',
            'ATMP','WTMP','DEWP','VIS','PTDY','TIDE']

        """

        link = "{}.{}".format(self.link, 'txt')
        #combine the first five date columns YY MM DD hh mm and make index
        df = pd.read_csv(link, delim_whitespace=True, na_values='MM',
            parse_dates=[[0,1,2,3,4]], index_col=0)

        try:
            #first column is units, so drop it
            df.drop(df.index[0], inplace=True)
            #convert the dates to datetimes
            df.index = pd.to_datetime(df.index,format="%Y %m %d %H %M")

            #convert to floats
            cols = ['WDIR','WSPD','GST','WVHT','DPD','APD','MWD',
            'PRES','ATMP','WTMP','DEWP','VIS','PTDY','TIDE']
            df[cols] = df[cols].astype(float)
        except:

            #convert the dates to datetimes
            df.index = pd.to_datetime(df.index,format="%Y %m %d %H %M")

            #convert to floats
            cols = ['WD','WSPD','GST','WVHT','DPD','APD','MWD','BARO',
            'ATMP','WTMP','DEWP','VIS','PTDY','TIDE']
            df[cols] = df[cols].astype(float)
        df.index.name='Date'
        return df

