import pandas as pd
from seebuoy import ndbc


def test_buoy_data():
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
        df = ndbc(buoy, d)
        # should return a df or None
        dtype = type(df)
        assert dtype == pd.core.frame.DataFrame or df is None
