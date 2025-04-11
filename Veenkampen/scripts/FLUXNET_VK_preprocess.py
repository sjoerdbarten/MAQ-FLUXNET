import pandas as pd
import numpy as np
import os
from datetime import timedelta,datetime
import netCDF4 as nc
import glob
from get_VK_meteo import *

process_year = 2024

#Setting GLOB
directory = 'W:\ESG\DOW_MAQ\MAQ_Archive\MAQ-Observations.nl\data\VK_FLUX\\'
files = sorted(glob.glob(directory + "VK_flux*.csv"))

#We need all files from process_year + Jan 01 from next year and Dec 31 from previous year
filtered_files = [
    file for file in files
    if file == f"{directory}VK_flux{str(int(process_year)-1)}1231.csv"
    or file.startswith(f"{directory}VK_flux{str(int(process_year))}")
    or file == f"{directory}VK_flux{str(int(process_year)+1)}0101.csv"
]

#Read data and set NaN
df = pd.concat([pd.read_csv(f, header=0, skiprows=[1], encoding="latin1") for f in filtered_files], ignore_index=True)
df = df.apply(pd.to_numeric, errors='ignore')
df = df.replace(['NaN'], np.nan)
df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], format='%Y-%m-%d %H:%M:%S')
df = df.sort_values(by="TIMESTAMP").reset_index(drop=True)

#Set TIMESTAMP_START AND TIMESTAMP_END
df['TIMESTAMP_END'] = df['TIMESTAMP'].dt.strftime('%Y%m%d%H%M')
df['TIMESTAMP_START'] = (df['TIMESTAMP'] - pd.Timedelta(minutes=30)).dt.strftime('%Y%m%d%H%M')

#Rearrange and drop irrelevant columns following from new datetime notation
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
meteo_data = get_vk_meteo_func(process_year)
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
begin_year = process_year - 1
end_year = process_year + 1
full_range = pd.date_range(start=f"{begin_year}12310000", end=f"{end_year}01020000", freq="30T")
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
    if year == str(process_year): #Save only process year
        df_year = merged_df[merged_df['TIMESTAMP_START'].str[:4] == year]
        output_path = os.path.join(output_dir, f"FLUXNET_VK_{year}.csv")
        df_year.to_csv(output_path, index=False)
        print(f"Saved: {output_path}")
