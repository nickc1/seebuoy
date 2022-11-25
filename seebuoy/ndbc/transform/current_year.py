import pandas as pd

def parse_current_year_months(txt):

    col_rename = {
        "Name": "name",
        "Last modified": "last_modified",
        "Size": "size",
        "Description": "description"
    }

    df = pd.read_html(txt)[0]
    df = df.dropna(subset="Last modified")

    df = df[list(col_rename)].rename(columns=col_rename)

    df["month"] = df["name"].str[:3]

    return df

def parse_avail_current_year(txt):

    df = pd.read_html(txt)[0]
    col_rename = {
        "Name": "name",
        "Last modified": "last_modified",
        "Size": "size",
        "Description": "description"
    }
    df = pd.read_html(txt)[0]
    df = df.dropna(subset="Last modified")
    df = df[list(col_rename)].rename(columns=col_rename)

    df["file_name"] = df["name"].str.split(".").str[0]
    
    # if in the current month, the files will not be gzipped and will
    # have a .txt extension instead of .txt.gz
    if len(df):
        file_extension = df["name"].str.split('.').str[-1].iloc[0]
    else:
        file_extension = None

    if file_extension == "txt":
        df["station_id"] = df["name"].str.split('.').str[0]
    else:
        df["station_id"] = df["file_name"].str[:-5]

    return df


def parse_all_avail_current_year(data):

    df_store = []
    for month, txt in data.items():

        df = parse_avail_current_year(txt)
        df["month_name"] = month
        df_store.append(df)
        
    
    return pd.concat(df_store)
