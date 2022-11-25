import pandas as pd
from .. import extract

def parse_avail_current_year_month(txt):

    df = pd.read_html(txt)[0]
    col_rename = {
        "Name": "file_name",
        "Last modified": "last_modified",
        "Size": "size",
        "Description": "description"
    }
    df = pd.read_html(txt)[0]
    df = df.dropna(subset="Last modified")
    df = df[list(col_rename)].rename(columns=col_rename)
    
    # if in the current month, the files will not be gzipped and will
    # have a .txt extension instead of .txt.gz
    if len(df):
        file_extension = df["file_name"].str.split('.').str[-1].iloc[0]
    else:
        file_extension = None

    if file_extension == "txt":
        df["station_id"] = df["file_name"].str.split('.').str[0]
    else:
        df["station_id"] = df["file_name"].str.split('.').str[0].str[:-5]

    return df


def parse_avail_current_year(data, dataset):

    file_ext = extract.HIST_DATASETS[dataset]

    df_store = []
    for month, txt in data.items():
        
        df = parse_avail_current_year_month(txt)
        df["month_name"] = month
        df["url"] = f"{file_ext}/{month}/" + df["file_name"]
        df["dataset"] = dataset
        df_store.append(df)
        
    
    return pd.concat(df_store)
