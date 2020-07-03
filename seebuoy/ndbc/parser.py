import requests
import csv
import re


def _make_request(url):

    # TODO: add error handling
    try:
        resp = requests.get(url)
    except:
        raise ValueError(f"Problem getting url: {url}")
    
    return resp

def _resp_to_dict(text):
    """Converts an ndbc url to a python dictionary
    This is based off pandas:
    https://github.com/pandas-dev/pandas/blob/master/pandas/io/parsers.py#L2475
    """

    rows = text.split('\n')

    pat = re.compile(r"\s+")

    # convert to list
    parsed = [pat.split(r.strip()) for r in rows]
    
    #fix hash in col names
    cols = [c.replace('#', '') for c in parsed[0]]

    #convert list to list of dicts
    x = [dict(zip(cols, p)) for p in parsed]

    # add date
    return x



