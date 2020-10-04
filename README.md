# 
<p align="center">
  <a href="#"><img src="https://raw.githubusercontent.com/nickc1/seebuoy/master/docs/img/seebuoy_logo_text.png" alt="seebuoy"></a>
</p>
<p align="center">
<em>Easily access data from the National Data Buoy Center.</em>
</p>

---

seebuoy dumps real time [NDBC](http://www.ndbc.noaa.gov) buoy data into [pandas](https://pandas.pydata.org/) dataframes. It's aim is to be easy to use:

```python
import seebuoy as sb

df = sb.ndbc('41013')
df.head()
```

 | date                |   WDIR |   WSPD |   GST |   WVHT |   DPD |   APD |   MWD |   PRES |   ATMP |   WTMP |   DEWP |   VIS |   PTDY |   TIDE |
|:--------------------|-------:|-------:|------:|-------:|------:|------:|------:|-------:|-------:|-------:|-------:|------:|-------:|-------:|
| 2020-09-26 19:50:00 |    280 |      3 |     4 |    1.1 |     7 |   5.9 |   159 | 1012.8 |   24.7 |   27.2 |   21.8 |   nan |    nan |    nan |
| 2020-09-26 18:50:00 |    290 |      3 |     4 |    1.1 |     7 |   5.7 |   166 | 1013.4 |   24.4 |   27   |   21.7 |   nan |    nan |    nan |
| 2020-09-26 17:50:00 |    310 |      2 |     3 |    1.1 |     7 |   5.6 |   165 | 1013.9 |   24.1 |   26.9 |   21.4 |   nan |    nan |    nan |
| 2020-09-26 16:50:00 |    350 |      3 |     5 |    1.2 |     7 |   5.6 |   164 | 1014.7 |  nan   |   26.8 |  nan   |   nan |    nan |    nan |
| 2020-09-26 16:40:00 |    340 |      3 |     5 |    1.2 |   nan |   5.6 |   164 | 1014.7 |   24   |   26.8 |   21.4 |   nan |    nan |    nan |


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
import seebuoy as sb

df = sb.ndbc('41013', data_set='supl')
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
<p align="center">

## Historical Data

Historical data can be found at:

https://www.ndbc.noaa.gov/station_history.php?station=41037


## Installation

```
pip install seebuoy
```





