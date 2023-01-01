# NDBC

Seebuoy provide's three main pieces of functionality for interacting with the [NDBC](http://www.ndbc.noaa.gov):

1. Stations: Information about all NDBC stations, buoys, and gliders.
2. Available data: What data is available for each buoy.
3. Get data: Retrieve data for the given buoy and dataset.



## Stations

The get information about NDBC's 1800+ stations you can use the `stations` method:

``` py
from seebuoy import NBDC

ndbc = NDBC()

df_buoys = ndbc.stations()
```

In addition to the data available from NDBC, seebuoy also adds on the closest cities, states, and the owner of the buoy. If you just want the raw data you can set the following arguements to false:

``` py
df_buoys = ndbc.stations(closest_cities=False, owners=False)

```

If you already know the buoy you are looking for, you can also pass that as a keyword argument:

``` py
df_buoys = ndbc.stations(station_id="41002")
```

## Available Data

Not all buoys have the same data available and some data that NDBC provides is from [ADCPs](https://en.wikipedia.org/wiki/Acoustic_Doppler_current_profiler). For this reason, it is useful to see what data is available for what buoys. You can access this through the `available_data` method below.

``` py
df_available = ndbc.available_data()
```

This will return the available data for all buoys for the default dataset. If you want to list all available data, you can use pass `dataset="all"`:

``` py
df_available = ndbc.available_data(dataset="all")
```

You can also subset the data to specific buoy by passing in a `station_id`:

``` py
df_available = ndbc.available_data(dataset="oceanographic", station_id="41002")
```


## Get Data

Finally when you are ready to retrieve data, you can use the `get_data` method:

``` py
station_id = "41002"
ndbc.get_data(station_id)
```

Similarly you can also specify a dataset you are interested in pulling:

``` py
ndbc.get_data(station_id, dataset="oceanographic")
```

The get data function also renames columns by default. For example, in the raw files from NDBC they pass "wvht" which maps to "wave_height". If you want the raw headers from the files, you can pass `rename_cols=False`:

``` py
ndbc.get_data(station_id, dataset="oceanographic", rename_cols=False)
```

## Historical Data

By default, seebuoy will default to only pulling real time data. If you want to pull historical data as well, you can initialize the class with `timeframe="historical"`.


``` py
from seebuoy import NBDC

ndbc = NDBC(timeframe="historical")
```

This pulls all available data, so be kind the NDBC servers. For example, to get all data going back to 1973 for buoy 41002, you can simply call the following:

``` py
df_avail = ndbc.available_data(station_id="41002")
df_data = ndbc.get_station("41002")
```

The above examples retrieves close to a half million observations. You can view the jupyter notebook for this example [here](https://google.com).


## Reference


::: seebuoy.ndbc.ndbc.NDBC
    handler: python
    options:
      members:
        - __init__
        - stations
        - available_data
        - get_data
      show_root_heading: true
      show_root_full_path: false
      show_source: false
      heading_level: 3


