import pandas as pd

def parse_current_year_months(txt):

    df = pd.read_html(txt)[0]

    return df