import pandas as pd
from seebuoy import NDBC


def test_stations():

    ndbc = NDBC()
    df = ndbc.stations()

    assert len(df) > 1000


def test_available_data():
    
    ndbc = NDBC()
    df = ndbc.available_data(dataset="all")
    
    assert len(df) > 100


def test_get_data():
    
    ndbc = NDBC()
    df = ndbc.get_station("41024", dataset="oceanographic")

    assert len(df) > 0


def test_historical():

    ndbc = NDBC(timeframe="historical")
    df_avail = ndbc.available_data(station_id="41002")
    df_data = ndbc.get_data("41002")

    assert len(df_avail) > 100
    assert len(df_data) > 100_000
