import netCDF4 as nc
import pandas as pd
import numpy as np
import glob
import os

def get_aams_meteo_func(process_year):
    #Setting GLOB
    directory = 'W:\ESG\DOW_MAQ\MAQ_Archive\MAQ-Observations.nl\data\AD_RAD\\' # Change this to the actual path
    files = sorted(glob.glob(directory + "Adam_rad*.csv"))

    #Filter variables
    variables = ['TIMESTAMP', 'Qs_in_Avg', 'Qs_out_Avg', 'T1_Avg', 'T2_Avg', 'LW_in', 'LW_out']

    #We need all files from process_year + Jan 01 from next year
    filtered_files = [
        file for file in files
        if file == f"{directory}Adam_rad{str(int(process_year)-1)}1231.csv"
        or file.startswith(f"{directory}Adam_rad{str(int(process_year))}")
        or file == f"{directory}Adam_rad{str(int(process_year)+1)}0101.csv"
    ]
    
    #Check if data exists and otherwise make empty df
    if filtered_files:
        #Read data and set NaN
        df = pd.concat([pd.read_csv(f, header=0, skiprows=[1], encoding="latin1") for f in filtered_files], ignore_index=True)
        df = df.apply(pd.to_numeric, errors='ignore')
        df = df.replace(['NaN'], np.nan)
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], format='%Y-%m-%d %H:%M:%S')
        df = df.sort_values(by="TIMESTAMP").reset_index(drop=True)
        df =  df[variables]
    else:
        adjusted_columns = []
        for var in variables:
            if var == 'TIMESTAMP':
                adjusted_columns.extend(['TIMESTAMP_START', 'TIMESTAMP_END'])
            else:
                adjusted_columns.append(var)

        df = pd.DataFrame(columns=adjusted_columns)
        
    #Unit conversion to FLUXNET format
    #NO UNIT CONVERSION NEEDED
    
    #Rename columns to FLUXNET format
    #Remember that for data processing, these variables are essential: FC, SC (only if measured with a profile) or CO2, SW_IN or PPFD, TA, TS, USTAR or TAU, RH. Very important are also H, LE, SWC and Precipitation.
    #Keep columns in the Variable Codes list https://www.europe-fluxdata.eu/home/guidelines/how-to-submit-data/variables-codes
    rename_dict = {
        'TIMESTAMP': 'TIMESTAMP',
        'Qs_in_Avg': 'SW_IN_1_1_1',
        'Qs_out_Avg': 'SW_OUT_1_1_1',
        'T1_Avg': 'TA_2_1_1',
        'T2_Avg': 'TA_3_1_1',
        'LW_in': 'LW_IN_1_1_1',
        'LW_out': 'LW_OUT_1_1_1'
    }
    df.rename(columns=rename_dict, inplace=True)
    
    if not filtered_files:
           return df
    
    
    #RESAMPLE TO 30-MIN AVERAGES (or sum for precipitation or max for gusts), use end-time as timestamp
    agg_dict = {col: lambda x: np.nanmean(x) for col in df.columns if col != 'TIMESTAMP'}
        
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

