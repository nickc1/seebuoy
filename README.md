# 
<p align="center">
  <a href="#"><img src="https://raw.githubusercontent.com/nickc1/seebuoy/master/docs/img/seebuoy_logo_text.png" alt="seebuoy"></a>
</p>
<p align="center">
<em>Easily access real time and historical data from the <a href="http://www.ndbc.noaa.gov">National Data Buoy Center</a>.</em>
</p>

---

seebuoy provides an easy to use python interface to the [National Data Buoy Center](http://www.ndbc.noaa.gov). Easily access realtime data, historical data, and metadata about the buoys.

## Quick Start

```python
from seebuoy import NDBC

ndbc = NDBC()

# Information on NDBC's ~1800 buoys and gliders
df_buoys = ndbc.stations()

# list all available data for all buoys
df_data = ndbc.available_data()

# get all data for a buoy
station_id = "41037"
df_buoy = ndbc.get_station(station_id)
```

See the [documentation](https://seebuoy.com/ndbc) for more detailed description.


## Installation

```
pip install seebuoy
```


## Other Resources

- [The Distributed Oceanographic Data Systems](https://dods.ndbc.noaa.gov)
- [Measurement Descriptions](https://www.ndbc.noaa.gov/measdes.shtml)
- [NDBC File Directory](https://www.ndbc.noaa.gov/data/)


