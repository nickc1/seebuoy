import pandas as pd
from seebuoy import ndbc


def test_real_time():
    datasets = [
        "data_spec",
        "ocean",
        "spec",
        "supl",
        "swdir",
        "swdir2",
        "swr1",
        "swr2",
        "txt",
    ]
    buoy = 41013

    for d in datasets:
        df = ndbc.real_time(buoy, d)
        # should return a df or None
        dtype = type(df)
        assert dtype == pd.core.frame.DataFrame or df is None


def test_available_downloads():

    df = ndbc.available_datasets(41037)
    assert len(df) > 0


def test_historic():
    df = ndbc.historic(41037, 2018, "stdmet")
    assert len(df) > 0
