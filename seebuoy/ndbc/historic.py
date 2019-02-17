

class Historic:

    def __init__(self, buoy, year, year_range=None):

        link = 'http://www.ndbc.noaa.gov/view_text_file.php?filename='
        link += '{}h{}.txt.gz&dir=data/historical/'.format(buoy, year)
        self.link = link

    def get_stand_meteo(self,link = None):
        '''
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
        '''

        link = self.link + 'stdmet/'

        #combine the first five date columns YY MM DD hh and make index
        df = pd.read_csv(link, header=0, delim_whitespace=True, dtype=object,
            na_values=[99,999,9999,99.,999.,9999.])


        #2007 and on format
        if df.iloc[0,0] =='#yr':


            df = df.rename(columns={'#YY': 'YY'}) #get rid of hash

            #make the indices
            
            df.drop(0, inplace=True) #first row is units, so drop them
            
            d = df.YY + ' ' + df.MM+ ' ' + df.DD + ' ' + df.hh + ' ' + df.mm
            ind = pd.to_datetime(d, format="%Y %m %d %H %M")

            df.index = ind

            #drop useless columns and rename the ones we want
            df.drop(['YY','MM','DD','hh','mm'], axis=1, inplace=True)
            df.columns = ['WDIR', 'WSPD', 'GST', 'WVHT', 'DPD', 'APD', 'MWD',
                'PRES', 'ATMP', 'WTMP', 'DEWP', 'VIS', 'TIDE']


        #before 2006 to 2000
        else:
            date_str = df.YYYY + ' ' + df.MM + ' ' + df.DD + ' ' + df.hh

            ind = pd.to_datetime(date_str,format="%Y %m %d %H")

            df.index = ind

            #some data has a minute column. Some doesn't.

            if 'mm' in df.columns:
                df.drop(['YYYY','MM','DD','hh','mm'], axis=1, inplace=True)
            else:
                df.drop(['YYYY','MM','DD','hh'], axis=1, inplace=True)


            df.columns = ['WDIR', 'WSPD', 'GST', 'WVHT', 'DPD', 'APD',
                'MWD', 'PRES', 'ATMP', 'WTMP', 'DEWP', 'VIS', 'TIDE']


        # all data should be floats
        df = df.astype('float')

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

        start,stop = self.year_range

        #see what is on the NDBC so we only pull the years that are available
        links = []
        for ii in range(start,stop+1):

            base = 'http://www.ndbc.noaa.gov/view_text_file.php?filename='
            end = '.txt.gz&dir=data/historical/stdmet/'
            link = base + str(self.buoy) + 'h' + str(ii) + end

            try:
                urllib2.urlopen(link)
                links.append(link)

            except:
                print(str(ii) + ' not in records')

        #need to also retrieve jan, feb, march, etc.
        month = ['Jan','Feb','Mar','Apr','May','Jun',
            'Jul','Aug','Sep','Oct','Nov','Dec']
        k = [1,2,3,4,5,6,7,8,9,'a','b','c'] #for the links

        for ii in range(len(month)):
            mid = '.txt.gz&dir=data/stdmet/'
            link = base + str(self.buoy) + str(k[ii]) + '2016' + mid + str(month[ii]) +'/'

            try:
                urllib2.urlopen(link)
                links.append(link)

            except:
                print(str(month[ii]) + '2016' + ' not in records')
                print(link)


        # start grabbing some data
        df=pd.DataFrame() #initialize empty df

        for L in links:

            new_df = self.get_stand_meteo(link=L)
            print('Link : ' + L)
            df = df.append(new_df)

        return df