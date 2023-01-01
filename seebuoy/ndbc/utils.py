import requests

BASE_URL = "https://www.ndbc.noaa.gov/data"


def get_url(url):

    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.text
    elif resp.status_code == 404:
        print(f"Dataset not available (404 Error) for url: \n {url}")
        return None
    else:
        raise ValueError(f"Error code {resp.status_code} for url: \n {url}")
