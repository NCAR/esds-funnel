import numpy as np
import xarray as xr

import funnel as fn


# TODO: move this kind of thing to a yaml file with other units info
C_flux_vars = [
    'FG_CO2', 'photoC_TOT_zint_100m', 'photoC_diat_zint_100m', 'photoC_diaz_zint_100m',
    'photoC_sp_zint_100m', 'POC_FLUX_100m',
]


@fn.register_derived_var(
    varname='pCFC11',
    dependent_vars=['CFC11', 'TEMP', 'SALT'],
)
def derive_var_pCFC11(ds):
    """compute pCFC11"""
    from calc import calc_cfc11sol

    ds['pCFC11'] = ds['CFC11'] * 1e-9 / calc_cfc11sol(ds.SALT, ds.TEMP)
    ds.pCFC11.attrs['long_name'] = 'pCFC-11'
    ds.pCFC11.attrs['units'] = 'patm'
    if 'coordinates' in ds.TEMP.attrs:
        ds.pCFC11.attrs['coordinates'] = ds.TEMP.attrs['coordinates']
    ds.pCFC11.encoding = ds.TEMP.encoding
    return ds.drop(['CFC11', 'TEMP', 'SALT'])


@fn.register_derived_var(
    varname='pCFC12',
    dependent_vars=['CFC12', 'TEMP', 'SALT'],
)
def derive_var_pCFC12(ds):
    """compute pCFC12"""
    from calc import calc_cfc12sol

    ds['pCFC12'] = ds['CFC12'] * 1e-9 / calc_cfc12sol(ds['SALT'],ds['TEMP'])
    ds.pCFC12.attrs['long_name'] = 'pCFC-12'
    ds.pCFC12.attrs['units'] = 'patm'
    if 'coordinates' in ds.TEMP.attrs:
        ds.pCFC12.attrs['coordinates'] = ds.TEMP.attrs['coordinates']
    ds.pCFC12.encoding = ds.TEMP.encoding
    return ds.drop(['CFC12', 'TEMP', 'SALT'])      


@fn.register_derived_var(
    varname='Cant', 
    dependent_vars=['DIC', 'DIC_ALT_CO2'],
)
def derive_var_Cant(ds):
    """compute Cant"""
    ds['Cant'] = ds['DIC'] - ds['DIC_ALT_CO2']
    ds.Cant.attrs = ds.DIC.attrs
    ds.Cant.attrs['long_name'] = 'Anthropogenic CO$_2$'

    if 'coordinates' in ds.DIC.attrs:
        ds.Cant.attrs['coordinates'] = ds.DIC.attrs['coordinates']
    ds.Cant.encoding = ds.DIC.encoding
    return ds.drop(['DIC', 'DIC_ALT_CO2'])


@fn.register_derived_var(
    varname='Del14C', 
    dependent_vars=['ABIO_DIC14', 'ABIO_DIC'],
)
def derive_var_Del14C(ds):
    """compute Del14C"""
    ds['Del14C'] = 1000. * (ds['ABIO_DIC14'] / ds['ABIO_DIC'] - 1.)
    ds.Del14C.attrs = ds.ABIO_DIC14.attrs
    ds.Del14C.attrs['long_name'] = '$\Delta^{14}$C'
    ds.Del14C.attrs['units'] = 'permille'    

    if 'coordinates' in ds.ABIO_DIC14.attrs:
        ds.Del14C.attrs['coordinates'] = ds.ABIO_DIC14.attrs['coordinates']
    ds.Del14C.encoding = ds.ABIO_DIC14.encoding
    return ds.drop(['ABIO_DIC14', 'ABIO_DIC'])


@fn.register_derived_var(
    varname='SST', 
    dependent_vars=['TEMP'],
)
def derive_var_SST(ds):
    """compute SST"""
    ds['SST'] = ds['TEMP'].isel(z_t=0, drop=True)
    ds.SST.attrs = ds.TEMP.attrs
    ds.SST.attrs['long_name'] = 'SST'
    ds.SST.encoding = ds.TEMP.encoding
    if 'coordinates' in ds.TEMP.attrs:
        ds.SST.attrs['coordinates'] = ds.TEMP.attrs['coordinates'].replace('z_t', '')   
    return ds.drop('TEMP')   


@fn.register_derived_var(
    varname='DOC_FLUX_IN_100m', 
    dependent_vars=[
        'DIA_IMPVF_DOC',
        'HDIFB_DOC',
        'WT_DOC',
    ],
)
def derive_var_DOC_FLUX_IN_100m(ds):
    """compute DOC flux across 100m (positive down)"""
    k_100m_top = np.where(ds.z_w_top == 100e2)[0][0]
    k_100m_bot = np.where(ds.z_w_bot == 100e2)[0][0]    
    DIA_IMPVF = ds.DIA_IMPVF_DOC.isel(z_w_bot=k_100m_bot)
    HDIFB = ds.HDIFB_DOC.isel(z_w_bot=k_100m_bot) * ds.dz[k_100m_bot]
    WT = (-1.0) * ds.WT_DOC.isel(z_w_top=k_100m_top) * ds.dz[k_100m_top]
    
    ds['DOC_FLUX_IN_100m'] = (DIA_IMPVF + HDIFB + WT)
    ds.DOC_FLUX_IN_100m.attrs = ds.WT_DOC.attrs
    ds.DOC_FLUX_IN_100m.attrs['long_name'] = 'DOC flux across 100 m (positive down)'
    ds.DOC_FLUX_IN_100m.attrs['units'] = 'nmol/s/cm^2'
    ds.DOC_FLUX_IN_100m.encoding = ds.WT_DOC.encoding
    return ds.drop(['DIA_IMPVF_DOC', 'HDIFB_DOC', 'WT_DOC'])   


@fn.register_derived_var(
    varname='DOCr_FLUX_IN_100m', 
    dependent_vars=[
        'DIA_IMPVF_DOCr',
        'HDIFB_DOCr',
        'WT_DOCr',
    ],
)
def derive_var_DOCr_FLUX_IN_100m(ds):
    """compute DOCr flux across 100m (positive down)"""
    k_100m_top = np.where(ds.z_w_top == 100e2)[0][0]
    k_100m_bot = np.where(ds.z_w_bot == 100e2)[0][0]    
    DIA_IMPVF = ds.DIA_IMPVF_DOCr.isel(z_w_bot=k_100m_bot)
    HDIFB = ds.HDIFB_DOCr.isel(z_w_bot=k_100m_bot) * ds.dz[k_100m_bot]
    WT = (-1.0) * ds.WT_DOCr.isel(z_w_top=k_100m_top) * ds.dz[k_100m_top]
    
    ds['DOCr_FLUX_IN_100m'] = (DIA_IMPVF + HDIFB + WT)
    ds.DOCr_FLUX_IN_100m.attrs = ds.WT_DOCr.attrs
    ds.DOCr_FLUX_IN_100m.attrs['long_name'] = 'DOCr flux across 100 m (positive down)'
    ds.DOCr_FLUX_IN_100m.attrs['units'] = 'nmol/s/cm^2'
    ds.DOCr_FLUX_IN_100m.encoding = ds.WT_DOCr.encoding
    return ds.drop(['DIA_IMPVF_DOCr', 'HDIFB_DOCr', 'WT_DOCr'])  


@fn.register_derived_var(
    varname='DOCt_FLUX_IN_100m', 
    dependent_vars=[
        'DIA_IMPVF_DOC',
        'HDIFB_DOC',
        'WT_DOC',        
        'DIA_IMPVF_DOCr',
        'HDIFB_DOCr',
        'WT_DOCr',
    ],
)
def derive_var_DOCt_FLUX_IN_100m(ds):
    """compute DOCt (DOC + DOCr) flux across 100m (positive down)"""
    k_100m_top = np.where(ds.z_w_top == 100e2)[0][0]
    k_100m_bot = np.where(ds.z_w_bot == 100e2)[0][0]    

    DIA_IMPVF = ds.DIA_IMPVF_DOC.isel(z_w_bot=k_100m_bot)
    HDIFB = ds.HDIFB_DOC.isel(z_w_bot=k_100m_bot) * ds.dz[k_100m_bot]
    WT = (-1.0) * ds.WT_DOC.isel(z_w_top=k_100m_top) * ds.dz[k_100m_top]
    
    DIA_IMPVF += ds.DIA_IMPVF_DOCr.isel(z_w_bot=k_100m_bot)
    HDIFB += ds.HDIFB_DOCr.isel(z_w_bot=k_100m_bot) * ds.dz[k_100m_bot]
    WT += (-1.0) * ds.WT_DOCr.isel(z_w_top=k_100m_top) * ds.dz[k_100m_top]
    
    ds['DOCt_FLUX_IN_100m'] = (DIA_IMPVF + HDIFB + WT)
    ds.DOCt_FLUX_IN_100m.attrs = ds.WT_DOC.attrs
    ds.DOCt_FLUX_IN_100m.attrs['long_name'] = 'Total DOC flux across 100 m (positive down)'
    ds.DOCt_FLUX_IN_100m.attrs['units'] = 'nmol/s/cm^2'
    ds.DOCt_FLUX_IN_100m.encoding = ds.WT_DOC.encoding   
    
    return ds.drop([
        'DIA_IMPVF_DOC', 'HDIFB_DOC', 'WT_DOC',
        'DIA_IMPVF_DOCr', 'HDIFB_DOCr', 'WT_DOCr'
    ])


@fn.register_derived_var(
    varname='DOCt', 
    dependent_vars=['DOC', 'DOCr'],
)
def derive_var_DOCt(ds):
    """compute DOCt"""
    ds['DOCt'] = ds['DOC'] + ds['DOCr']
    ds.DOCt.attrs = ds.DOC.attrs
    ds.DOCt.attrs['long_name'] = 'Dissolved Organic Carbon (total)'
    ds.DOCt.encoding = ds.DOC.encoding
    return ds.drop(['DOC', 'DOCr'])


@fn.register_derived_var(
    varname='DONt', 
    dependent_vars=['DON', 'DONr'],
)
def derive_var_DONt(ds):
    """compute DONt"""
    ds['DONt'] = ds['DON'] + ds['DONr']
    ds.DONt.attrs = ds.DON.attrs
    ds.DONt.attrs['long_name'] = 'Dissolved Organic Nitrogen (total)'
    ds.DONt.encoding = ds.DON.encoding
    return ds.drop(['DON', 'DONr'])


@fn.register_derived_var(
    varname='DOPt', 
    dependent_vars=['DOP', 'DOPr'],
)
def derive_var_DOPt(ds):
    """compute DOPt"""
    ds['DOPt'] = ds['DOP'] + ds['DOPr']
    ds.DOPt.attrs = ds.DOP.attrs
    ds.DOPt.attrs['long_name'] = 'Dissolved Organic Phosphorus (total)'
    ds.DOPt.encoding = ds.DOP.encoding
    return ds.drop(['DOP', 'DOPr'])
