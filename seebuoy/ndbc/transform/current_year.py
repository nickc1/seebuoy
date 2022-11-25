import pandas as pd
from .. import extract


def _build_txt_url(name, suffix, month):
    base_url = "https://www.ndbc.noaa.gov/view_text_file.php?filename"
    url = f"{base_url}={name}&dir=data/{suffix}/{month}/"
    
    return url


def parse_avail_current_year_month(txt, dataset, month):

    file_suffix = extract.HIST_DATASETS[dataset]

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
        df["file_extension"] = file_extension
        
    else:
        df["station_id"] = df["file_name"].str.split('.').str[0].str[:-5]
        df["file_extension"] = df["file_name"].str.split(".").str[-2]
    

    df["file_suffix"] = file_suffix
    df["url"] = f"{file_suffix}/{month}/" + df["file_name"]
    df["month_name"] = month
    df["dataset"] = dataset
    if file_extension == "txt":
        df["txt_url"] = extract.BASE_URL + "/" + df["url"]
    else:
        df["txt_url"] = df.apply(lambda row: _build_txt_url(row['file_name'], row['file_suffix'], row["month_name"]), axis=1)
    return df


def parse_avail_current_year(data, dataset):

    df_store = []
    for month, txt in data.items():
        
        df = parse_avail_current_year_month(txt, dataset, month)

        df_store.append(df)
        
    
    return pd.concat(df_store)
