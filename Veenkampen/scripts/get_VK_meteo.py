import netCDF4 as nc
import pandas as pd
import numpy as np
import glob
import os

def get_vk_meteo_func(process_year):
    #Setting GLOB
    directory = 'W:\ESG\DOW_MAQ\MAQ_Archive\MAQ-Observations.nl\data\VK_METEO\\' # Change this to the actual path
    files = sorted(glob.glob(directory + "VK_meteo*.csv"))

    #We need all files from process_year + Jan 01 from next year
    filtered_files = [
        file for file in files
        if file == f"{directory}VK_meteo{str(int(process_year)-1)}1231.csv"
        or file.startswith(f"{directory}VK_meteo{str(int(process_year))}")
        or file == f"{directory}VK_meteo{str(int(process_year)+1)}0101.csv"
    ]

    #Read data and set NaN
    df = pd.concat([pd.read_csv(f, header=0, skiprows=[1], encoding="latin1") for f in filtered_files], ignore_index=True)
    df = df.apply(pd.to_numeric, errors='ignore')
    df = df.replace(['NaN'], np.nan)
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], format='%Y-%m-%d %H:%M:%S')
    df = df.sort_values(by="TIMESTAMP").reset_index(drop=True)
    
    #Filter variables
    variables = ['TIMESTAMP',
        'TA_2_1_1', 'TA_1_1_1', 'TA_1_2_1', 'TA_1_1_2', 'RH_1_1_1',
        'SW_IN_1_1_1', 'SW_OUT_1_1_1', 'LW_IN_1_1_1', 'LW_OUT_1_1_1', 'SW_DIF_1_1_1',
        'P_1_1_7', 'PA_1_1_1',
        'WS_2_1_1', 'WX_2_1_1', 'WS_1_1_1', 'WX_1_1_1', 'WD_1_1_1', 'WS_1_2_1', 'WX_1_2_1', 'WD_1_2_1',
        'WTD_1_1_1', 'TS_1_1_1', 'TS_1_2_1', 'TS_1_3_1', 'TS_1_4_1', 'TS_1_5_1', 'TS_1_6_1',
        'TS_2_1_1', 'TS_2_2_1', 'TS_2_3_1', 'TS_2_4_1', 'G_1_1_1', 'G_2_1_1', 'G_3_1_1', 'G_4_1_1',
        'VWC_1_1_1', 'VWC_1_2_1', 'VWC_1_3_1', 'VWC_1_4_1',
        'VWC_2_1_1', 'VWC_2_2_1', 'VWC_2_3_1', 'VWC_2_4_1', 'VWC_3_1_1'      
    ]

    df =  df[variables]
    
    #Unit conversion to FLUXNET format
    df['VWC_1_1_1'] = df['VWC_1_1_1']*100.    #- to %
    df['VWC_1_2_1'] = df['VWC_1_2_1']*100.    #- to %
    df['VWC_1_3_1'] = df['VWC_1_3_1']*100.    #- to %
    df['VWC_1_4_1'] = df['VWC_1_4_1']*100.    #- to %
    df['VWC_2_1_1'] = df['VWC_2_1_1']*100.    #- to %
    df['VWC_2_2_1'] = df['VWC_2_2_1']*100.    #- to %
    df['VWC_2_3_1'] = df['VWC_2_3_1']*100.    #- to %
    df['VWC_2_4_1'] = df['VWC_2_4_1']*100.    #- to %
    df['VWC_3_1_1'] = df['VWC_3_1_1']*100.    #- to %
    
    #Rename columns to FLUXNET format
    #Remember that for data processing, these variables are essential: FC, SC (only if measured with a profile) or CO2, SW_IN or PPFD, TA, TS, USTAR or TAU, RH. Very important are also H, LE, SWC and Precipitation.
    #Keep columns in the Variable Codes list https://www.europe-fluxdata.eu/home/guidelines/how-to-submit-data/variables-codes
    rename_dict = {
        'TIMESTAMP': 'TIMESTAMP',
        'TA_2_1_1': 'TA_3_1_1',
        'TA_1_1_1': 'TA_2_1_1',
        'TA_1_2_1': 'TA_2_2_1',
        'TA_1_1_2': 'TA_2_1_2',
        'RH_1_1_1': 'RH_2_1_1',
        'SW_IN_1_1_1': 'SW_IN_1_1_1',
        'SW_OUT_1_1_1': 'SW_OUT_1_1_1',
        'LW_IN_1_1_1': 'LW_IN_1_1_1',
        'LW_OUT_1_1_1': 'LW_OUT_1_1_1',
        'SW_DIF_1_1_1': 'SW_DIF_1_1_1',
        'P_1_1_7': 'P_1_1_1',
        'PA_1_1_1': 'PA_2_1_1',
        'WS_2_1_1': 'WS_3_1_1',
        'WX_2_1_1': 'WS_MAX_3_1_1',
        'WS_1_1_1': 'WS_2_1_1',
        'WX_1_1_1': 'WS_MAX_2_1_1',
        'WD_1_1_1': 'WD_2_1_1',
        'WS_1_2_1': 'WS_2_2_1',
        'WX_1_2_1': 'WS_MAX_2_2_1',
        'WD_1_2_1': 'WD_2_2_1',
        'WTD_1_1_1': 'WTD_1_1_1',
        'TS_1_1_1': 'TS_1_1_1',
        'TS_1_2_1': 'TS_1_2_1',
        'TS_1_3_1': 'TS_1_3_1',
        'TS_1_4_1': 'TS_1_4_1',
        'TS_1_5_1': 'TS_1_5_1',
        'TS_1_6_1': 'TS_1_6_1',
        'TS_2_1_1': 'TS_2_1_1',
        'TS_2_2_1': 'TS_2_2_1',
        'TS_2_3_1': 'TS_2_3_1',
        'TS_2_4_1': 'TS_2_4_1',
        'G_1_1_1': 'G_1_1_1',
        'G_2_1_1': 'G_2_1_1',
        'G_3_1_1': 'G_3_1_1',
        'G_4_1_1': 'G_4_1_1',
        'VWC_1_1_1': 'SWC_1_1_1',
        'VWC_1_2_1': 'SWC_1_2_1',
        'VWC_1_3_1': 'SWC_1_3_1',  
        'VWC_1_4_1': 'SWC_1_4_1',
        'VWC_2_1_1': 'SWC_2_1_1',
        'VWC_2_2_1': 'SWC_2_2_1',
        'VWC_2_3_1': 'SWC_2_3_1',
        'VWC_2_4_1': 'SWC_2_4_1',
        'VWC_3_1_1': 'SWC_3_1_1',
    }
    df.rename(columns=rename_dict, inplace=True)
    
    
    #RESAMPLE TO 30-MIN AVERAGES (or sum for precipitation or max for gusts), use end-time as timestamp
    agg_dict = {col: lambda x: np.nanmean(x) for col in df.columns if col != 'TIMESTAMP'}
    agg_dict.update({
        'P_1_1_1': lambda x: np.nansum(x),
        'WS_MAX_3_1_1': lambda x: np.nanmax(x),
        'WS_MAX_2_1_1': lambda x: np.nanmax(x),
        'WS_MAX_2_2_1': lambda x: np.nanmax(x),
    })
        
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])    
    
    df['TIMESTAMP'] = df['TIMESTAMP'].dt.floor('30T')
    time_range = pd.date_range(df['TIMESTAMP'].min(), df['TIMESTAMP'].max(), freq='30T')
    empty_df = pd.DataFrame({'TIMESTAMP': time_range})
    resampled_df = df.groupby('TIMESTAMP', as_index=False).agg(agg_dict)
    resampled_df = empty_df.merge(resampled_df, on='TIMESTAMP', how='left')
    resampled_df['TIMESTAMP'] += pd.Timedelta(minutes=30)
        
    resampled_df['TIMESTAMP_END'] = resampled_df['TIMESTAMP'].dt.strftime('%Y%m%d%H%M')
    resampled_df['TIMESTAMP_START'] = (resampled_df['TIMESTAMP'] - pd.Timedelta(minutes=30)).dt.strftime('%Y%m%d%H%M')

    #Drop and rearrange
    resampled_df.drop(columns=['TIMESTAMP'], inplace=True)
    cols = ['TIMESTAMP_START', 'TIMESTAMP_END'] + [col for col in resampled_df.columns if col not in ['TIMESTAMP_START', 'TIMESTAMP_END']]
    resampled_df = resampled_df[cols]
    
    return resampled_df

