# 
<p align="center">
  <a href="#"><img src="https://raw.githubusercontent.com/nickc1/seebuoy/master/docs/img/seebuoy_logo_text.png" alt="seebuoy"></a>
</p>
<p align="center">
<em>Easily access oceanographic data.</em>
</p>
<p align="center">
  <a href="https://seebuoy.com">DOCUMENTATION</a>
</p>
---

seebuoy dumps real time and historic data buoy data from the [NDBC](http://www.ndbc.noaa.gov) into [pandas](https://pandas.pydata.org/) dataframes. It's aim is to be easy to use:

```python
from seebuoy import ndbc

df = ndbc.real_time('41013')
df.head()
```

 | date                |   WDIR |   WSPD |   GST |   WVHT |   DPD |   APD |   MWD |   PRES |   ATMP |   WTMP |   DEWP |   VIS |   PTDY |   TIDE |
|:--------------------|-------:|-------:|------:|-------:|------:|------:|------:|-------:|-------:|-------:|-------:|------:|-------:|-------:|
| 2020-09-26 19:50:00 |    280 |      3 |     4 |    1.1 |     7 |   5.9 |   159 | 1012.8 |   24.7 |   27.2 |   21.8 |   nan |    nan |    nan |
| 2020-09-26 18:50:00 |    290 |      3 |     4 |    1.1 |     7 |   5.7 |   166 | 1013.4 |   24.4 |   27   |   21.7 |   nan |    nan |    nan |
| 2020-09-26 17:50:00 |    310 |      2 |     3 |    1.1 |     7 |   5.6 |   165 | 1013.9 |   24.1 |   26.9 |   21.4 |   nan |    nan |    nan |
| 2020-09-26 16:50:00 |    350 |      3 |     5 |    1.2 |     7 |   5.6 |   164 | 1014.7 |  nan   |   26.8 |  nan   |   nan |    nan |    nan |
| 2020-09-26 16:40:00 |    340 |      3 |     5 |    1.2 |   nan |   5.6 |   164 | 1014.7 |   24   |   26.8 |   21.4 |   nan |    nan |    nan |


See the [documentation](https://seebuoy.com/ndbc) for more detailed description.

## Installation

```
pip install seebuoy
```



## Other Resources

- [The Distributed Oceanographic Data Systems](https://dods.ndbc.noaa.gov)


