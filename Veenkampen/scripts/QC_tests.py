#This script serves as QC for submitted FLUXNET data. European Fluxes Database Cluster Straff noted issues with the data (see mail 2025-08-06 11:02) also pasted below.
#Here we do some plotting/analysis to check some of these issues.
'''
Dear Sjoerd,
We are checking your dataset to start the data processing, and there are some issues that need to be corrected before proceeding with the next steps:
- The radiation analysis shows a 2-hour shift forward compared to the local time of the 2018 timestamp. This issue prevents the file from being imported and the subsequent steps from being performed. It is therefore necessary to correct and resubmit the file, paying attention to the synchronisation of both weather variables and fluxes.
- TAU must always be negative by convention. We will correct this issue.
- WTD must always be negative by convention. We will correct this issue.
- There are also a number of issues concerning the trend of certain variables: 
CO2 and FC are very noisy even when applying Foken's quality flags, and this will lower the quality of the final output. 
PA, LW_IN, LW_OUT, WS_3_1_1 and WS_MAX_3_1_1 have periods that will be eliminated from the dataset (see plots). These issues are less of a priority because they do not prevent data processing.
- Finally, a note on metadata and, in particular, on filling the BADM Instrument_Ops template. Information must be provided separately for each individual variable. For each variable, the model and serial number of the instrument must be provided. Fluxes variables should not be mapped, except for those that can be measured by meteo instruments, such as TA, RH, PA, WS, WD, etc. I am attaching an example of how to fill in the form to help you understand. Please fill in one BADM form and submit it in the PI area.
Waiting for your feedback, for further assistance do not hesitate to contact me again.
Best regards,
Eleonora Canfora
European Fluxes Database Cluster Staff
'''

import pandas as pd
import glob
import os
import matplotlib.pyplot as plt
import numpy as np
from math import sin, cos, acos, asin, tan, radians, degrees, pi


data_folder = "W:\ESG\DOW_MAQ\MAQ_Archive\Veenkampen_archive\FLUXNET\Veenkampen\data"
csv_files = glob.glob(os.path.join(data_folder, "*.csv"))
df = pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)
df = df.replace(-9999.0, np.nan)

print(df.shape)
print(df.head())
print(df.columns)

df["TIMESTAMP_START"] = pd.to_datetime(df["TIMESTAMP_START"], format="%Y%m%d%H%M")

#CHECK VARIABLES WITH PERIODS THAT WILL BE ELIMINATED
for var in ["PA_1_1_1","LW_IN_1_1_1","LW_OUT_1_1_1","FC_1_1_1","CO2_1_1_1","WS_3_1_1","WS_MAX_3_1_1","WS_2_1_1","WS_MAX_2_1_1"]:
    plt.figure(figsize=(14,6))
    plt.plot(df["TIMESTAMP_START"], df[var], 
             marker=".", linestyle="none", markersize=2, alpha=0.7)
    plt.xlabel("Time")
    plt.ylabel(var)
    plt.title(var+" Time Series")
    plt.grid(True)
    plt.show()

#PA_1_1_1 (EC System air_pressure). Flatlines from end-2023, also in raw data.
#LW_IN_1_1_1. Small period in 2023 with faulty data.
#LW_OUT_1_1_1. Small period in 2023 with faulty data.
#FC_1_1_1. Noise part of measurments.
#CO2_1_1_1. Noise part of measurments.
#WS_3_1_1. Faulty data from 2021 onwards, also in raw data
#WS_3_1_1. Faulty data from 2021 onwards, also in raw data

#Not much to do here for FLUXNET submission, data needs to be filtered out (except FC, CO2) needs to be filtered out in original datasets.

#Now on to the most pressing issue: 2-hour time shift of radiation data:
#CONCLUSION OF RADIATION ANALYSIS:
# IT DOES NOT LOOK LIKE 2018 IS SHIFTED COMPARED TO OTHER YEARS. THESE PLOTS HAVE BEEN COMMUNICATED WITH EFDC.

# ----------------------------
# 1) Clean & prepare dataframe
# ----------------------------
# Keep only the needed columns (adjust if your df already filtered)
cols = ["TIMESTAMP_START","SW_IN_1_1_1","SW_OUT_1_1_1","LW_IN_1_1_1","LW_OUT_1_1_1"]
dfc = df[cols].copy()

# Extract year, month, hour
dfc["year"] = dfc["TIMESTAMP_START"].dt.year
dfc["month"] = dfc["TIMESTAMP_START"].dt.month
dfc["hour"]  = dfc["TIMESTAMP_START"].dt.hour

# ----------------------------
# 2) Diurnal climatology
# ----------------------------
vars_to_plot = ["SW_IN_1_1_1","SW_OUT_1_1_1","LW_IN_1_1_1","LW_OUT_1_1_1"]
g = dfc.groupby(["year","month","hour"])[vars_to_plot].mean().reset_index()

# ----------------------------
# 3) Potential max SW_IN (TOA horizontal)
# ----------------------------
LAT = 51.98   # Wageningen latitude
LON = 5.67    # Wageningen longitude
LON_STD = 15  # CET central meridian (UTC+1)
G_SC = 1367.0 # Solar constant

def day_of_year(dt):
    return int(dt.strftime("%j"))

def declination(n):
    return 23.45 * sin(2*pi*(284 + n)/365.0)

def eot_minutes(n):
    B = 2*pi*(n - 81)/364.0
    return 9.87*np.sin(2*B) - 7.53*np.cos(B) - 1.5*np.sin(B)

def potential_rad_for_month(month, year=2024):
    """
    Potential TOA horizontal irradiance at local standard time (no DST).
    """
    n = day_of_year(pd.Timestamp(year=year, month=month, day=15))  # mid-month
    delta = radians(declination(n))
    phi   = radians(LAT)
    EoT   = eot_minutes(n)
    # time correction (hours)
    tc = (EoT + 4*(LON - LON_STD)) / 60.0
    G_on = G_SC * (1 + 0.033 * cos(2*pi*n/365.0))

    hours = np.arange(24)
    pot = []
    for h in hours:
        solar_time = h + tc + 0.5
        omega = radians(15*(solar_time - 12))
        cosz = sin(phi)*sin(delta) + cos(phi)*cos(delta)*cos(omega)
        pot.append(G_on*cosz if cosz > 0 else 0.0)
    return hours, np.array(pot)

monthly_potential = {m: potential_rad_for_month(m) for m in range(1, 13)}

# ----------------------------
# 4) Plotting
# ----------------------------
month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
hours = np.arange(24)

for var in vars_to_plot:
    fig, axes = plt.subplots(3, 4, figsize=(18, 10), sharex=True, sharey=False)
    axes = axes.flatten()
    handles_seen = {}

    for m in range(1, 13):
        ax = axes[m-1]
        ax.set_title(month_names[m-1])
        gm = g[g["month"] == m]
        years = sorted(gm["year"].unique())

        # plot all years except 2018 first
        for y in [yy for yy in years if yy != 2018]:
            sub = gm[gm["year"] == y].groupby("hour")[var].mean().reindex(hours)
            lh, = ax.plot(hours, sub.values, lw=1, alpha=0.8, label=str(y))
            if str(y) not in handles_seen:
                handles_seen[str(y)] = lh

        # plot 2018 last in black
        if 2018 in years:
            sub18 = gm[gm["year"] == 2018].groupby("hour")[var].mean().reindex(hours)
            lh18, = ax.plot(hours, sub18.values, lw=2, color="black", label="2018")
            handles_seen["2018"] = lh18

        # overlay potential radiation for SW_IN
        if var == "SW_IN_1_1_1":
            h, pot = monthly_potential[m]
            lpot, = ax.plot(h, pot, "k--", lw=2, label="Potential SW_IN (TOA)")
            if "Potential SW_IN (TOA)" not in handles_seen:
                handles_seen["Potential SW_IN (TOA)"] = lpot

        ax.set_xlim(0, 23)
        ax.set_xticks([0, 6, 12, 18])
        ax.grid(True, alpha=0.3)
        if m in [1,5,9]:
            ax.set_ylabel(var)
        if m in [9,10,11,12]:
            ax.set_xlabel("Hour (local standard time)")

    fig.suptitle(f"Average Diurnal Cycle per Month — {var} (all years, 2018 emphasized)", fontsize=16)
    fig.legend(handles_seen.values(), labels=handles_seen.keys(),
               loc="lower center", ncols=min(6, len(handles_seen)), bbox_to_anchor=(0.5, -0.02))
    plt.tight_layout(rect=[0, 0.03, 1, 0.96])
    plt.show()
