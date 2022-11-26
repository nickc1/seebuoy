from . import recent
from . import current_year
from . import historic


def standard(txt, data_group, rename_cols=True):

    if data_group == "recent":
        df = recent.standard(txt, rename_cols=rename_cols)
    
    else:
        df = historic.standard(txt, rename_cols=rename_cols)
    
    return df


def oceanographic(txt, data_group, rename_cols=True):

    if data_group == "recent":
        df = recent.oceanographic(txt, rename_cols=rename_cols)
    
    else:
        df = historic.oceanographic(txt, rename_cols=rename_cols)
    
    return df

def supplemental(txt, data_group, rename_cols=True):

    if data_group == "recent":
        df = recent.supplemental(txt, rename_cols=rename_cols)
    
    else:
        df = historic.supplemental(txt, rename_cols=rename_cols)
    
    return df


def raw_spectral(txt, data_group, rename_cols=True):

    if data_group == "recent":
        df = recent.raw_spectral(txt, rename_cols=rename_cols)
    
    else:
        df = historic.raw_spectral(txt, rename_cols=rename_cols)
    
    return df


def spectral_alpha1(txt, data_group, rename_cols=True):

    if data_group == "recent":
        df = recent.spectral_alpha1(txt, rename_cols=rename_cols)
    
    else:
        df = historic.spectral_alpha1(txt, rename_cols=rename_cols)
    
    return df


def spectral_alpha2(txt, data_group, rename_cols=True):

    if data_group == "recent":
        df = recent.spectral_alpha2(txt, rename_cols=rename_cols)
    
    else:
        df = historic.spectral_alpha2(txt, rename_cols=rename_cols)
    
    return df


def spectral_r1(txt, data_group, rename_cols=True):

    if data_group == "recent":
        df = recent.spectral_r1(txt, rename_cols=rename_cols)
    
    else:
        df = historic.spectral_r1(txt, rename_cols=rename_cols)
    
    return df


def spectral_r2(txt, data_group, rename_cols=True):

    if data_group == "recent":
        df = recent.spectral_r2(txt, rename_cols=rename_cols)
    
    else:
        df = historic.spectral_r2(txt, rename_cols=rename_cols)
    
    return df