# NDBC

seebuoy dumps real time and historic [NDBC](http://www.ndbc.noaa.gov) buoy data into [pandas](https://pandas.pydata.org/) dataframes. It's aim is to be easy to use:

```python
from seebuoy import ndbc

df = ndbc.real_time('41013')
df.head()
```

| date                |   wdir |   wspd |   gst |   wvht |   dpd |   apd |   mwd |   pres |   atmp |   wtmp |   dewp |   vis |   ptdy |   tide |
|:--------------------|-------:|-------:|------:|-------:|------:|------:|------:|-------:|-------:|-------:|-------:|------:|-------:|-------:|
| 2021-01-16 22:50:00 |    280 |     12 |    17 |    2.8 |     8 |   5.7 |   216 | 1008.5 |    nan |   18.9 |    nan |   nan |    nan |    nan |
| 2021-01-16 22:40:00 |    270 |     13 |    16 |  nan   |   nan | nan   |   nan | 1008.3 |    nan |   18.9 |    nan |   nan |    nan |    nan |

## Data Files
Most buoys have the following data files associated with them:

|File         | Parameters
|-------------|----------
| .data_spec  | Raw Spectral Wave Data
| .ocean      | Oceanographic Data
| .spec       | Spectral Wave Summary Data
| .supl       | Supplemental Measurements Data
| .swdir      | Spectral Wave Data (alpha1)
| .swdir2     | Spectral Wave Data (alpha2)
| .swr1       | Spectral Wave Data (r1)
| .swr2       | Spectral Wave Data (r2)
| .txt        | Standard Meteorological Data


You can find definitions for each of these on the [NDBC website](http://www.ndbc.noaa.gov/measdes.shtml).

You can pull any of these with seebuoy:

```python
df = ndbc.real_time('41013', data_set='supl')
df.head()
```


 | #YY_MM_DD_hh_mm     |   PRES |   PTIME |   WSPD |   WDIR |   WTIME |
|:--------------------|-------:|--------:|-------:|-------:|--------:|
| 2020-08-12 00:40:00 |    nan |     nan |      6 |    nan |      37 |
| 2020-08-12 00:30:00 |    nan |     nan |      6 |    nan |      23 |
| 2020-08-12 00:20:00 |    nan |     nan |      7 |    nan |      17 |
| 2020-08-12 00:10:00 |    nan |     nan |      6 |    nan |       4 |
| 2020-08-12 00:00:00 |    nan |     nan |      6 |    nan |    2351 |

The realtime data for all of their buoys can be found [here](http://www.ndbc.noaa.gov/data/realtime2/).

Info about all of noaa data can be found [here](http://www.ndbc.noaa.gov/docs/ndbc_web_data_guide.pdf).

## Buoy Numbers

To find the number of a buoy you can check the [ndbc](http://www.ndbc.noaa.gov) home page:

<p align="center">
  <a href="http://www.ndbc.noaa.gov"><img src="https://raw.githubusercontent.com/nickc1/seebuoy/master/docs/img/ndbc_map.png" alt="seebuoy"></a>
</p>


## Historical Data

To get the available historical data for a buoy, you can run:

```python
df_avail = ndbc.available_downloads('41037')
df_avail.head()
```

|    |   year | month   | dataset   | url                                                                      | date                |
|---:|-------:|:--------|:----------|:-------------------------------------------------------------------------|:--------------------|
| 31 |   2008 | Jan     | adcp      | /download_data.php?filename=41037a2008.txt.gz&dir=data/historical/adcp/  | 2008-01-01 00:00:00 |
| 32 |   2009 | Jan     | ocean     | /download_data.php?filename=41037o2009.txt.gz&dir=data/historical/ocean/ | 2009-01-01 00:00:00 |
| 33 |   2010 | Jan     | ocean     | /download_data.php?filename=41037o2010.txt.gz&dir=data/historical/ocean/ | 2010-01-01 00:00:00 |


Then to retrieve the data:

```python
df = ndbc.historic('41037', 2019)
```

 | date                |   WDIR |   WSPD |   GST |   WVHT |   DPD |   APD |   MWD |   PRES |   ATMP |   WTMP |   DEWP |   VIS |   PTDY |   TIDE |
|:--------------------|-------:|-------:|------:|-------:|------:|------:|------:|-------:|-------:|-------:|-------:|------:|-------:|-------:|
| 2020-09-26 19:50:00 |    280 |      3 |     4 |    1.1 |     7 |   5.9 |   159 | 1012.8 |   24.7 |   27.2 |   21.8 |   nan |    nan |    nan |
| 2020-09-26 18:50:00 |    290 |      3 |     4 |    1.1 |     7 |   5.7 |   166 | 1013.4 |   24.4 |   27   |   21.7 |   nan |    nan |    nan |
| 2020-09-26 17:50:00 |    310 |      2 |     3 |    1.1 |     7 |   5.6 |   165 | 1013.9 |   24.1 |   26.9 |   21.4 |   nan |    nan |    nan |
| 2020-09-26 16:50:00 |    350 |      3 |     5 |    1.2 |     7 |   5.6 |   164 | 1014.7 |  nan   |   26.8 |  nan   |   nan |    nan |    nan |
| 2020-09-26 16:40:00 |    340 |      3 |     5 |    1.2 |   nan |   5.6 |   164 | 1014.7 |   24   |   26.8 |   21.4 |   nan |    nan |    nan |


If you want to pull a subset of years:

```python
years = ndbc.available_years('41037', "stdmet")
df_store = []
for year in years[-3:]:
  df_store.append(ndbc.historic('41037', year, dataset="stdmet"))

df = pd.concat(df_store)
```

There is also a convenience function that can pull all available data. **This can pull a large amount of data so be kind to ndbc.**

```python
df_all = ndbc.all_historic('41037', dataset="stdmet")
```

When you pull data from a large year range, it is possible that the data format has changed, so you might need to do some cleaning. Here is a full real world example:

```python
import pandas as pd
import matplotlib.pyplot as plt
from seebuoy import ndbc

buoy = 41013
dataset = "stdmet"

df = ndbc.all_historic(buoy, dataset)

df_wvht = df['wvht'].dropna()

fig, ax = plt.subplots(figsize=(12, 6))
df_wvht.plot(ax=ax)
df_wvht.resample("w").mean().plot(ax=ax)
df_wvht.resample("m").mean().plot(ax=ax)

```

<p align="center">
  <img src="/img/wvht_41013_stdmet.png" alt="seebuoy">
</p>