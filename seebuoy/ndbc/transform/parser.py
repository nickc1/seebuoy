from . import recent
from . import current_year
from . import historic


def standard(txt, data_group, rename_cols=True):

    if data_group == "recent":
        df = recent.standard(txt, rename_cols=rename_cols)
    
    else:
        df = historic.standard(txt, rename_cols=rename_cols)
    
    return df

