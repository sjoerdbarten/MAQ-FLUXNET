import netCDF4 as nc
import pandas as pd
import numpy as np

def get_hw_meteo_func():
    dataset = nc.Dataset('W:/ESG/DOW_MAQ/MAQ_Archive/zz_HaarwegDuivendaal/Haarweg_processed_data/NETCDF_ALLDATA/Hweg2001_2012_30min.nc','r')

    #Variables to load in
    variables = [
        'Sin_avg', 'Sout_avg', 'Lin_avg', 'Lout_avg', 'rain_sum', 'T005_grass_avg', 'T010_grass_avg', 'T020_grass_avg', 'T050_grass_avg', 'T100_grass_avg',
        'SHF005_grass_avg', 'T005_bare_avg', 'T010_bare_avg', 'T020_bare_avg', 'SHF005_bare_avg',
        'T150dv_avg', 'RH150hair_avg', 'T150vais_avg', 'RH150vais_avg',
        'U10cup_avg', 'U10cup_max', 'WD10vane_avg'
    ]

    #Set datetime values
    t_YYYY = dataset.variables['t_YYYY'][:]
    t_DOY = dataset.variables['t_DOY'][:]
    t_HHMM = dataset.variables['t_HHMM'][:]

    t_YYYY = np.ma.filled(t_YYYY, 0).astype(int)
    t_DOY = np.ma.filled(t_DOY, 0).astype(int)
    t_HHMM = np.ma.filled(t_HHMM, 0).astype(int)
    
    #Make dataframe
    data = {var: dataset.variables[var][:] for var in variables}
    df = pd.DataFrame(data)

    #Convert datetime
    t_YYYY_series = pd.Series(t_YYYY.astype(str))
    t_DOY_series = pd.Series(t_DOY.astype(str)).str.zfill(3)
    df['date'] = pd.to_datetime(t_YYYY_series + t_DOY_series, format='%Y%j')
    
    t_HHMM_series = pd.Series(t_HHMM.astype(str)).str.zfill(4)
    df['time'] = df['date'] + pd.to_timedelta(t_HHMM_series.str[:2].astype(int), unit='H')
    df['time'] = df['time'] + pd.to_timedelta(t_HHMM_series.str[2:].astype(int), unit='m')
    df['time'] = df['time'].dt.strftime('%Y-%m-%d %H:%M')
    df = df[['time'] + [col for col in df.columns if col != 'time']]

    df['time'] = pd.to_datetime(df['time'])
    df['TIMESTAMP_END'] = df['time'].dt.strftime('%Y%m%d%H%M')
    df['TIMESTAMP_START'] = (df['time'] - pd.Timedelta(minutes=30)).dt.strftime('%Y%m%d%H%M')

    #Drop and rearrange
    df.drop(columns=['date','time'], inplace=True)
    cols = ['TIMESTAMP_START', 'TIMESTAMP_END'] + [col for col in df.columns if col not in ['TIMESTAMP_START', 'TIMESTAMP_END']]
    df = df[cols]
    
    #Unit conversion to FLUXNET format
    #NO UNIT CONVERSION NEEDED
    
    #Rename columns to FLUXNET format
    #Remember that for data processing, these variables are essential: FC, SC (only if measured with a profile) or CO2, SW_IN or PPFD, TA, TS, USTAR or TAU, RH. Very important are also H, LE, SWC and Precipitation.
    #Keep columns in the Variable Codes list https://www.europe-fluxdata.eu/home/guidelines/how-to-submit-data/variables-codes
    rename_dict = {
        'TIMESTAMP_START': 'TIMESTAMP_START',
        'TIMESTAMP_END': 'TIMESTAMP_END',
        'Sin_avg': 'SW_IN_1_1_1',
        'Sout_avg': 'SW_OUT_1_1_1',
        'Lin_avg': 'LW_IN_1_1_1',
        'Lout_avg': 'LW_OUT_1_1_1',
        'rain_sum': 'P_1_1_1',
        'T005_grass_avg': 'TS_1_1_1',
        'T010_grass_avg': 'TS_1_2_1',
        'T020_grass_avg': 'TS_1_3_1',
        'T050_grass_avg': 'TS_1_4_1',
        'T100_grass_avg': 'TS_1_5_1',
        'SHF005_grass_avg': 'G_1_1_1',
        'T005_bare_avg': 'TS_2_1_1',
        'T010_bare_avg': 'TS_2_2_1',
        'T020_bare_avg': 'TS_2_3_1',
        'SHF005_bare_avg': 'G_2_1_1',
        'T150dv_avg': 'TA_2_1_1',
        'RH150hair_avg': 'RH_2_1_1',
        'T150vais_avg': 'TA_3_1_1',
        'RH150vais_avg': 'RH_3_1_1',
        'U10cup_avg': 'WS_2_1_1',
        'U10cup_max': 'WS_MAX_2_1_1',
        'WD10vane_avg': 'WD_2_1_1',
    }
    df.rename(columns=rename_dict, inplace=True)

    dataset.close()
    
    return df

