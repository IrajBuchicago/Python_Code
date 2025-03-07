#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Date Created: 2nd Jan 2025
Author: Muhammad Bhatti
Code Objective: 
    Match mauza shapefile with population census
"""
# =============================================================================
# Setup
# =============================================================================

import pandas as pd
from fuzzywuzzy import fuzz  # Ensure you have fuzzywuzzy installed
from fuzzywuzzy import process
import getpass
import os
import pandas as pd
import geopandas as gpd

username = getpass.getuser() #Get the username of your device

 # For new user, please add your directory below.
if username == "muhammad":
    work_dir = "/Users/muhammad/Dropbox/Research/Floods-2010/"
elif username == "muhammadbinkhalid":
    work_dir = "/Users/muhammadbinkhalid/Dropbox (Harvard University)/Floods-2010/"
elif username == "mub200":
    work_dir = "C:/Users/mub200/Dropbox (Harvard University)/Floods-2010/"

os.chdir(work_dir)

# =============================================================================
# Cleaning
# =============================================================================

# Step 1: Merge Saarim's pop crosswalk w my census cw. 
### That assigns shapefile to 50 percent of the data.
### For the remaining 50 pc, match the shapefile w the data.

# 2017 census wave
file_path = "population_panel/data/intermediate/census_98_23/2017/all_dist_2017.csv"
census_2017 = pd.read_csv(file_path)

census_2017['cen_id'] = range(1, len(census_2017) + 1)

# Sindh mauza shapefile 
file_path = 'mauza_sf/new_sf/combined_shapefile/sindh_mouzas_combined.shp'
#file_path = 'mauza_sf/Sindh_mauza_shapefile/combined_shapefile/sindh_mouzas_combined.shp'
mauza_sf_all = gpd.read_file(file_path)
mauza_sf = mauza_sf_all

### Step 1: Use the cw earlier developed based on Saarim's census - get the 50 pc of shapefiles matched

## Sf-crosswalk w Saarim's data:
    
#file_path = 'data/intermediate/census/cw_mauza_sf_p1_pop_census_2017.csv'
file_path = 'population_panel/data/intermediate/cw_mauza_sf_p1_pop_census_2017.csv'
sf_cw = pd.read_csv(file_path)

# Split the column 'dist_maz_census' into two new columns: 'district' and 'taluka'
sf_cw[['district_cen', 'taluka_cen']] = sf_cw['dist_maz_census'].str.split('_', n=2, expand=True)[[0, 1]]

# Remove any spaces in the new columns and create a con column
sf_cw['district_cen'] = sf_cw['district_cen'].str.replace(' ', '')
sf_cw['taluka_cen'] = sf_cw['taluka_cen'].str.replace(' ', '')

sf_cw['con'] = sf_cw['district_cen'] + '_' + sf_cw['taluka_cen'] + '_' + sf_cw['mauza_census']


## Some cleaning on census data
census_17 = census_2017

# Changing district and taluka names to match the cleaning done on Saarim's data
census_17['district'] = census_17['district'].str.replace('MIRPUR KHAS', 'MIRPURKHAS')
census_17['district'] = census_17['district'].str.replace('SHAHEED BENAZIRABAD', 'BENAZIRABAD')

census_17['taluka'] = census_17['taluka'].str.replace(' ', '')

taluka_mapping = {
    'TANDOMUHAMMADKHAN': 'T.M.KHAN',
    'SHUJAABAD': 'SHUJABAD',
    'GOLARCHI(S.F.RAHU)': 'SHAHEEDFAZALRAHU',
    'KOTGHULAMMUHAMMAD': 'K.G.M.B',
    'MANJHAND': 'MANJAHND',
    'TANDOGHULAMHYDER': 'T.GH.HYDER',
    'KHAIRPURNATHANSHAH': 'NATHANSHAH',
    'CHAMBER': 'CHAMBAR',
    'LARKANA': 'LARKANO'
}

census_17['taluka'] = census_17['taluka'].replace(taluka_mapping)

census_17['district'] = census_17['district'].str.replace(' ', '')
census_17['mauza_name'] = census_17['mauza_name'].str.replace(' ', '')

census_17['con'] = census_17['district'] + '_' + census_17['taluka'] + '_' + census_17['mauza_name']
census_17 = census_17[['con', 'cen_id']]

sf_cw1 = sf_cw[['con', 'sf_id', 'dist_maz_sf', 'dist_maz_census', 'best_score']]

# Now, merging the two datasets tgt on con
merged_df = census_17.merge(sf_cw1, on='con')

duplicates = merged_df[merged_df.duplicated(subset='sf_id', keep=False)]

merger = mauza_sf.merge(merged_df, left_on='mauza_id', right_on='sf_id')

merger = merger[['mauza_id', 'cen_id', 'best_score']]

## Now, merging these back for the final dataset.
merged_sf = mauza_sf

merged_sf = merged_sf.merge(merger, on='mauza_id', how='left')
merged_sf = merged_sf.dropna(subset=['cen_id'])


### New matching begins from here onwards

# Identifying the remaining unmatched shapefile mauzas and census mauzas
mauza_sf = mauza_sf[~mauza_sf['mauza_id'].isin(merged_sf['mauza_id'])]
cw_17 = census_2017[~census_2017['cen_id'].isin(merged_sf['cen_id'])]

cw_17 = cw_17[['district', 'taluka', 'mauza_name', 'cen_id']]

# Harmonizing district and tehsil names

# Cleaning mauza shapefile
mauza_sf = mauza_sf.dropna(subset=['mauza_name'])
mauza_sf['district'] = mauza_sf['district'].str.upper().str.replace(" ", "")
mauza_sf['taluka'] = mauza_sf['taluka'].str.upper().str.replace(" ", "")
mauza_sf['mauza_name'] = mauza_sf['mauza_name'].str.upper().str.replace(" ", "")

mauza_sf = mauza_sf[['district', 'taluka', 'mauza_name', 'mauza_id']]

cw_17['district'] = cw_17['district'].str.upper().str.replace(" ", "")
cw_17['taluka'] = cw_17['taluka'].str.upper().str.replace(" ", "")
cw_17['mauza_name'] = cw_17['mauza_name'].str.upper().str.replace(" ", "")


dist_sf = mauza_sf['district'].unique()
dist_cw = cw_17['district'].unique()

not_in_sf = set(dist_cw) - set(dist_sf)
not_in_cw = set(dist_sf) - set(dist_cw)

print(not_in_sf)
print(not_in_cw)

# There are still some unmatched mouzas in sf from Hyderabad and Shikarpur, but none from census - leave that aside for now. Will get back to these. 
cw_17['district'] = cw_17['district'].str.replace('MALIR', 'KARACHIMALIR')
cw_17['district'] = cw_17['district'].str.replace('KAMBARSHAHDADKOT', 'QAMBAR')

# Drop Karachi
cw_17 = cw_17[~cw_17['district'].isin(['KARACHIWEST', 'KARACHISOUTH', 'KARACHICENTRAL', 'KARACHIEAST'])]
mauza_sf = mauza_sf[~mauza_sf['district'].isin(['SHIKARPUR', 'HYDERABAD'])]


## Taluka name harmonizing
taluka_sf = mauza_sf['taluka'].unique()
taluka_cw = cw_17['taluka'].unique()

common_talukas = set(taluka_sf).intersection(taluka_cw)
not_in_cw = set(taluka_sf) - set(taluka_cw)
not_in_sf = set(taluka_cw) - set(taluka_sf)

print(not_in_sf)
print(not_in_cw)

taluka_mapping = {
    'SHAHBUNDER': 'SHAHBUNDAR',
    'UMERKOT': 'UMARKOT',
    'SANGHAR': 'SANGHRR',
    'MEHRABPUR': 'MEHARABPUR',
    'TANGWANI': 'TARGWANI',
    'KAMBARALIKHAN': 'KAMBER',
    'JAMNAWAZALI': 'JAMMAWAZALI',
    'MIRPURSAKRO': 'MIRPURSAKHRO',
    'NAUSHAHROFEROZE': 'NAUSHAHROFEROZ',
    'KHAROCHAN': 'KHAROCHAAN'
}

cw_17['taluka'] = cw_17['taluka'].replace(taluka_mapping)


# =============================================================================
# Matching Round 1
# =============================================================================

mauza_sf['dist_maz'] = mauza_sf['district'] + '_' + mauza_sf['taluka'] + "_" + mauza_sf['mauza_name']
mauza_sf['dist_tal'] = mauza_sf['district'] + '_' + mauza_sf['taluka']

mauza_sf = mauza_sf.dropna(subset=['dist_maz'])
mauza_sf = mauza_sf[~mauza_sf['dist_maz'].duplicated(keep=False)]

# Handling duplicates in district_taluka_mauza combination in census
cw_17['dist_maz'] = cw_17['district'] + '_' + cw_17['taluka'] + "_" + cw_17['mauza_name']
cw_17['dist_tal'] = cw_17['district'] + '_' + cw_17['taluka']

cw_17 = cw_17.dropna(subset=['dist_maz'])
# Remove rows where 'dist_maz' is duplicated
cw_17 = cw_17[~cw_17['dist_maz'].duplicated(keep=False)]



mauzas_sf = mauza_sf['dist_maz'].unique()
mauzas_master = cw_17['dist_maz'].unique()
common_mauzas = set(mauzas_sf).intersection(mauzas_master)
not_in_sf = set(mauzas_master) - set(mauzas_sf)
not_in_master = set(mauzas_sf) - set(mauzas_master)

not_in_master = list(not_in_master)
# 1048 perfect matches! Great. 


matched_mauzas = []  # Create an empty list to store matched mauzas

for mauza_con in not_in_master:
    tal = mauza_sf[mauza_sf['dist_maz'] == mauza_con]['dist_tal'].values[0]
    
    # Create a subset of mauzas from census that are not already matched
    unmatched_mauzas = cw_17[(cw_17['dist_tal'] == tal) & (~cw_17['dist_maz'].isin(common_mauzas))]['mauza_name'].unique()
    
    mauza = mauza_con.split('_')[-1]
    
    # Perform fuzzy match to find the closest name similarity
    match_result = process.extractOne(mauza, unmatched_mauzas, scorer=fuzz.token_sort_ratio)
    
    if match_result:  # Ensure there is a valid match
        best_match, best_score = match_result

        # Get the full dist_maz of the best match
        best_match_dist_maz = cw_17[(cw_17['dist_tal'] == tal) & (cw_17['mauza_name'] == best_match)]['dist_maz'].values[0]

        # Append the result to the matched mauzas list
        matched_mauzas.append([mauza, best_match, best_score, mauza_con, best_match_dist_maz])
        print(f"'{mauza}' --> '{best_match}' with a score of {best_score}")
    else:
        print(f"No match found for '{mauza}' in taluka '{tal}'.")


matched_df = pd.DataFrame(matched_mauzas, columns=['mauza_sf', 'mauza_census', 'best_score', 'dist_maz_sf', 'dist_maz_census'])


# NASRAT all wrong, HIRAL all wrong, DIM also (below 80), DAD below 80 as well. Everything below and incl 67 is wrong as well

# Remove rows where 'best_score' is less than or equal to 67
matched_df = matched_df[matched_df['best_score'] > 67]

matched_df = matched_df[~((matched_df['best_score'] < 80) & 
                            (matched_df['mauza_sf'].str.contains('DIM|DAD', case=False, na=False)))]

matched_df = matched_df[~matched_df['mauza_sf'].str.contains('NASRAT', case=False, na=False)]

## Adding ids for fuzzy name matching
matched_df = matched_df.merge(mauza_sf[['mauza_id','dist_maz']], left_on='dist_maz_sf', right_on='dist_maz', how='left')
matched_df = matched_df.merge(cw_17[['cen_id','dist_maz']], left_on='dist_maz_census', right_on='dist_maz', how='left')
matched_df = matched_df[['mauza_id', 'cen_id', 'mauza_sf','mauza_census', 'dist_maz_sf', 'dist_maz_census', 'best_score']]

# Adding perfectly matched mauzas
common_mauzas_list = list(common_mauzas)
common_entries = mauza_sf[mauza_sf['dist_maz'].isin(common_mauzas_list)].merge(
    cw_17[cw_17['dist_maz'].isin(common_mauzas_list)], left_on='dist_maz', right_on='dist_maz'
)

new_entries = pd.DataFrame({
    'mauza_id': common_entries['mauza_id'],
    'cen_id': common_entries['cen_id'],
    'mauza_sf': common_entries['mauza_name_x'],
    'mauza_census': common_entries['mauza_name_x'],
    'dist_maz_sf': common_entries['dist_maz'],
    'dist_maz_census': common_entries['dist_maz'],
    'best_score': [100] * len(common_entries)
})

matched_df_all = pd.concat([matched_df, new_entries], ignore_index=True)

merger = matched_df_all[['mauza_id', 'cen_id', 'best_score']]
merger = mauza_sf_all.merge(merger, on = 'mauza_id', how = 'left')
merger = merger.dropna(subset=['cen_id'])

merged_sf = pd.concat([merged_sf, merger], ignore_index=True)

# =============================================================================
# Matching Round 2
# =============================================================================

#### Identifying the unmatched mauzas
mauza_sf = mauza_sf[~mauza_sf['mauza_id'].isin(merged_sf['mauza_id'])] # 686/6077 (11 pc) remaining
cw_17 = census_2017[~census_2017['cen_id'].isin(merged_sf['cen_id'])] # 792/5738 (14 pc) remaining

cw_17 = cw_17[['district', 'taluka', 'mauza_name', 'cen_id']]
mauza_sf = mauza_sf[['district', 'taluka', 'mauza_name', 'mauza_id']]



mauza_sf = mauza_sf.dropna(subset=['mauza_name'])
mauza_sf = mauza_sf[~mauza_sf['mauza_name'].duplicated(keep=False)]
mauza_sf['mauza_name'] = mauza_sf['mauza_name'].str.upper().str.replace(" ", "")

cw_17 = cw_17.dropna(subset=['mauza_name'])
cw_17 = cw_17[~cw_17['mauza_name'].duplicated(keep=False)]
cw_17['mauza_name'] = cw_17['mauza_name'].str.upper().str.replace(" ", "")

mauzas_sf = mauza_sf['mauza_name'].unique()
mauzas_master = cw_17['mauza_name'].unique()
common_mauzas = set(mauzas_sf).intersection(mauzas_master)
not_in_sf = set(mauzas_master) - set(mauzas_sf)
not_in_master = set(mauzas_sf) - set(mauzas_master)

not_in_master = list(not_in_master)


matched_mauzas = []  # Create an empty list to store matched mauzas

for mauza in not_in_master:

    # Create a subset of mauzas from census that are not already matched
    unmatched_mauzas = cw_17[~cw_17['mauza_name'].isin(common_mauzas)]['mauza_name'].unique()
    
    # Perform fuzzy match to find the closest name similarity
    match_result = process.extractOne(mauza, unmatched_mauzas, scorer=fuzz.token_sort_ratio)
    
    if match_result:  # Ensure there is a valid match
        best_match, best_score = match_result

        # Append the result to the matched mauzas list
        matched_mauzas.append([mauza, best_match, best_score])
        print(f"'{mauza}' --> '{best_match}' with a score of {best_score}")
    else:
        print(f"No match found for '{mauza}'")


matched_df = pd.DataFrame(matched_mauzas, columns=['mauza_sf', 'mauza_census', 'best_score'])

matched_df = matched_df[matched_df['best_score'] > 70]
matched_df = matched_df[~matched_df['mauza_sf'].str.contains('NASRAT', case=False, na=False)]

## Adding ids for fuzzy name matching
matched_df = matched_df.merge(mauza_sf[['mauza_id','mauza_name']], left_on='mauza_sf', right_on='mauza_name', how='left')
matched_df = matched_df.merge(cw_17[['cen_id','mauza_name']], left_on='mauza_census', right_on='mauza_name', how='left')
matched_df = matched_df[['mauza_id', 'cen_id', 'mauza_sf','mauza_census', 'best_score']]

# Adding perfectly matched mauzas
common_mauzas_list = list(common_mauzas)
common_entries = mauza_sf[mauza_sf['mauza_name'].isin(common_mauzas_list)].merge(
    cw_17[cw_17['mauza_name'].isin(common_mauzas_list)], left_on='mauza_name', right_on='mauza_name'
)

new_entries = pd.DataFrame({
    'mauza_id': common_entries['mauza_id'],
    'cen_id': common_entries['cen_id'],
    'mauza_sf': common_entries['mauza_name'],
    'mauza_census': common_entries['mauza_name'],
    'best_score': [100] * len(common_entries)
})

matched_df_all = pd.concat([matched_df, new_entries], ignore_index=True)
merger = matched_df_all[['mauza_id', 'cen_id', 'best_score']]
merger = mauza_sf_all.merge(merger, on = 'mauza_id', how = 'left')
merger = merger.dropna(subset=['cen_id'])

merged_sf = pd.concat([merged_sf, merger], ignore_index=True)


#### Identifying unmatched mauzas
mauza_sf = mauza_sf[~mauza_sf['mauza_id'].isin(merged_sf['mauza_id'])] # 232/6077 (4 pc) remaining
cw_17 = census_2017[~census_2017['cen_id'].isin(merged_sf['cen_id'])] # 585/5738 (8 pc) remaining


merged_sf = merged_sf[['mauza_id', 'cen_id', 'best_score']]
merged_sf.to_csv('population_panel/data/intermediate/cw_shapefile_census_17_new.csv', index=False)


