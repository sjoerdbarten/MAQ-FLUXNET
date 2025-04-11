import pandas as pd
import numpy as np
import os
from datetime import timedelta
import netCDF4 as nc
from get_HW_meteo import *

#Read data and set NaN
df = pd.read_csv("W:\ESG\DOW_MAQ\MAQ_Archive\zz_HaarwegDuivendaal\Haarweg_processed_data\ECdata\HwegEC2001_2011_30min.csv",
                 skiprows=0, header=1, sep=",", low_memory=False).drop(index=[0]).reset_index(drop=True)
df = df.apply(pd.to_numeric, errors='ignore')
df = df.replace([-9999, -9999.0, -9999.000000, 0.0000000000000000], np.nan)

#Set TIMESTAMP_START AND TIMESTAMP_END
df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'], format='%Y-%m-%d %H:%M')
df['TIMESTAMP_END'] = df['datetime'].dt.strftime('%Y%m%d%H%M')
df['TIMESTAMP_START'] = (df['datetime'] - pd.Timedelta(minutes=30)).dt.strftime('%Y%m%d%H%M')

#Rearrange and drop irrelevant columns following from new datetime notation
df.drop(columns=['filename','datetime','date','time','DOY'], inplace=True)
cols = ['TIMESTAMP_START', 'TIMESTAMP_END'] + [col for col in df.columns if col not in ['TIMESTAMP_START', 'TIMESTAMP_END']]
df = df[cols]

#Rename columns to FLUXNET format
#Remember that for data processing, these variables are essential: FC, SC (only if measured with a profile) or CO2, SW_IN or PPFD, TA, TS, USTAR or TAU, RH. Very important are also H, LE, SWC and Precipitation.
#Keep columns in the Variable Codes list https://www.europe-fluxdata.eu/home/guidelines/how-to-submit-data/variables-codes
cols = ['TIMESTAMP_START','TIMESTAMP_END','Tau','qc_Tau','H','qc_H',
        'LE','qc_LE','co2_flux','qc_co2_flux','h2o_flux',
        'H_strg','LE_strg','co2_strg',
        'co2_mixing_ratio','h2o_mole_fraction',
        'sonic_temperature','air_temperature','air_pressure',
        'RH','VPD','Tdew','wind_speed','max_wind_speed','wind_dir',
        'u*','L','(z-d)/L','x_70%','x_90%']
df = df[cols]
#Unit conversion to FLUXNET format
df['sonic_temperature'] = df['sonic_temperature']-273.15    #K to degC
df['air_temperature'] = df['air_temperature']-273.15    #K to degC
df['Tdew'] = df['Tdew']-273.15    #K to degC
df['air_pressure'] = df['air_pressure']*0.001    #Pa to kPa
df['VPD'] = df['VPD']*0.01    #Pa to hPa
#Rename the columns to FLUXNET format
rename_dict = {
    'TIMESTAMP_START': 'TIMESTAMP_START',
    'TIMESTAMP_END': 'TIMESTAMP_END',
    'Tau': 'TAU_1_1_1',
    'qc_Tau': 'TAU_SSITC_TEST_1_1_1',
    'H': 'H_1_1_1',
    'qc_H': 'H_SSITC_TEST_1_1_1',
    'LE': 'LE_1_1_1',
    'qc_LE': 'LE_SSITC_TEST_1_1_1',
    'co2_flux': 'FC_1_1_1',
    'qc_co2_flux': 'FC_SSITC_TEST_1_1_1',
    'h2o_flux': 'FH2O_1_1_1',
    'H_strg': 'SH_1_1_1',
    'LE_strg': 'SLE_1_1_1',
    'co2_strg': 'SC_1_1_1',
    'co2_mixing_ratio': 'CO2_1_1_1',
    'h2o_mole_fraction': 'H2O_1_1_1',
    'sonic_temperature': 'T_SONIC_1_1_1',
    'air_temperature': 'TA_1_1_1',
    'air_pressure': 'PA_1_1_1',
    'RH': 'RH_1_1_1',
    'VPD': 'VPD_PI_1_1_1',
    'Tdew': 'T_DP_1_1_1',
    'wind_speed': 'WS_1_1_1',
    'max_wind_speed': 'WS_MAX_1_1_1',
    'wind_dir': 'WD_1_1_1',
    'u*': 'USTAR_1_1_1',
    'L': 'MO_LENGTH_1_1_1',
    '(z-d)/L': 'ZL_1_1_1',
    'x_70%': 'FETCH_70_1_1_1',
    'x_90%': 'FETCH_90_1_1_1'
}
df.rename(columns=rename_dict, inplace=True)

#Get missing ancillary data from Meteo files
print('Finished processing fluxes')
print('Processing meteorology and soil data')
meteo_data = get_hw_meteo_func()
print('METEO DATAFRAME')
print(meteo_data)
print('FLUXES DATAFRAME')
print(df)
print('MERGING METEO AND FLUXES DATAFRAMES')

#Merge flux and meteo dataframes, using flux as reference dataframe but fill for completeness
meteo_data["TIMESTAMP_START"] = pd.to_datetime(meteo_data["TIMESTAMP_START"], format="%Y%m%d%H%M")
meteo_data["TIMESTAMP_END"] = pd.to_datetime(meteo_data["TIMESTAMP_END"], format="%Y%m%d%H%M")
df["TIMESTAMP_START"] = pd.to_datetime(df["TIMESTAMP_START"], format="%Y%m%d%H%M")
df["TIMESTAMP_END"] = pd.to_datetime(df["TIMESTAMP_END"], format="%Y%m%d%H%M")
full_range = pd.date_range(start=f"200012310000", end=f"201201020000", freq="30T")
full_df = pd.DataFrame({"TIMESTAMP_START": full_range})
full_df["TIMESTAMP_END"] = full_df["TIMESTAMP_START"] + pd.Timedelta(minutes=30)
merged_df = full_df.merge(df, on=["TIMESTAMP_START", "TIMESTAMP_END"], how="left")\
            .merge(meteo_data, on=["TIMESTAMP_START", "TIMESTAMP_END"], how="left")
merged_df = merged_df.sort_values("TIMESTAMP_START").reset_index(drop=True)

#Reformat time to FLUXNET Time convention: report the local time without “Daylight Saving Time”.
merged_df['TIMESTAMP_START'] = pd.to_datetime(merged_df['TIMESTAMP_START'], format='%Y%m%d%H%M') + timedelta(hours=1)
merged_df['TIMESTAMP_END'] = pd.to_datetime(merged_df['TIMESTAMP_END'], format='%Y%m%d%H%M') + timedelta(hours=1)
merged_df['TIMESTAMP_START'] = merged_df['TIMESTAMP_START'].dt.strftime('%Y%m%d%H%M')
merged_df['TIMESTAMP_END'] = merged_df['TIMESTAMP_END'].dt.strftime('%Y%m%d%H%M')

#Fill NaN with -9999 (FLUXNET FORMAT)
merged_df.fillna('-9999', inplace=True)

print('MERGED DATAFRAME')
print(merged_df)

#Split by year and save
output_dir = "../data"
os.makedirs(output_dir, exist_ok=True)

#Save yearfiles
for year in merged_df['TIMESTAMP_START'].str[:4].unique():
    if str(year) != '2000' and str(year) != '2012': #Do not save 2000 and 2012 (occured due to time shift)
        df_year = merged_df[merged_df['TIMESTAMP_START'].str[:4] == year]
        output_path = os.path.join(output_dir, f"FLUXNET_HW_{year}.csv")
        df_year.to_csv(output_path, index=False)
        print(f"Saved: {output_path}")
