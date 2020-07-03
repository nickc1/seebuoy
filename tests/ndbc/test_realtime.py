from seebuoy.ndbc.real_time import buoy_data


def test_buoy_data():
    datasets = [
        "dataspec",
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

    for d in data:
        df = buoy_data(buoy, d)   


