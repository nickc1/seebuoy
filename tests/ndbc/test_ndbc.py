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
        assert len(df) > 0
