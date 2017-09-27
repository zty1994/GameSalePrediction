# -*- coding: utf-8 -*-
"""
UVic SENG 474 Lab project data merge script

Created on Tue Sep 26 16:42:44 2017

@author: Pabloski
"""

import pandas as pd

SALES_FILENAME = "vgsales.csv"
CRITICS_FILENAME = "ign.csv"
OUTPUT_FILENAME = "merging_output.csv"

# We rename the 'title' and 'platform' 
# columns from the IGN file so they are merged
# with the equivalent columns in the sales file
IGN_TO_VG_COLUMN_NAMES = {
        'title':'Name', 
        'platform':'Platform'
        }

# We delete some columns from the result
CUT_COLUMNS = [
        'Year', # Repeated
        'Rank', # Dependent on other records
        'Unnamed: 0', # Number of record in IGN file
        'url' # Irrelevant
        ]

# We also substitute the platform names used
# in the IGN file by the ones used in the sales file
IGN_TO_VG_PLATFORM_NAMES = {
        'WonderSwan':'WS',
        'TurboGrafx-16':'TG16',
        'Dreamcast':'DC',
        'Sega CD':'SCD',
        # 'Sega Game Gear':'GG', # <- Not in IGN names
        'Saturn':'SAT',
        'Atari 2600':'2600',
        'Super NES':'SNES',
        'Nintendo 64':'N64',
        # Nintendo 64DD = N64 too?
        'Nintendo DS':'DS',
        'Nintendo 3DS':'3DS',
        'Wii U':'WiiU',
        'GameCube':'GC',
        'Game Boy':'GB',
        'Game Boy Advance':'GBA',
        'PlayStation':'PS',
        'PlayStation 2':'PS2',
        'PlayStation 3':'PS3',
        'PlayStation 4':'PS4',
        'PlayStation Portable':'PSP',
        'PlayStation Vita':'PSV',
        'Xbox':'XB',
        'Xbox 360':'X360',
        'Xbox One':'XOne',
        'NeoGeo':'NG',
        
        'PC':'PC',
        'Wii':'Wii',
        'NES':'NES'
        }

def check_platform_translation(ign_to_vg_map, critics, sales):
    # We announce all platform names we are not 
    # translating
    not_translated = ([
            x for x in critics['Platform'].unique()
            if x not in ign_to_vg_map
            and x not in ign_to_vg_map.values()
        ])
            
    if len(not_translated) > 0:
        message = "{} IGN platform names were not translated: {}"
        message = message.format(
                len(not_translated), 
                not_translated)
        print(message)
        print()
    
    # We also announce any VG name we are not 
    # translating into
    not_translated_into = ([
            x for x in sales['Platform'].unique()
            if x not in ign_to_vg_map.values()
        ])
        
    if len(not_translated_into) > 0:
        message = "{} VG platform names were not used as translation: {}"
        message = message.format(
                len(not_translated_into), 
                not_translated_into)
        print(message)
        print()
        
def translate_platforms(ign_to_vg_map, critics):
    critics['Platform'].replace(
            ign_to_vg_map, 
            inplace=True
            )

def merge(critics_filename, sales_filename, output_filename):
    sales = pd.read_csv(sales_filename)
    critics = pd.read_csv(critics_filename)

    # We rename the columns used for matching rows from 
    # the sales file
    critics = critics.rename(columns=IGN_TO_VG_COLUMN_NAMES)
    
    # We show information about possible translation misconfigurations
    check_platform_translation(IGN_TO_VG_PLATFORM_NAMES, critics, sales)

    # We merge the data in function of the columns
    # sharing the same name, excluding partial matches
    output = sales.merge(critics, how="inner")
    
    print("Size of the sales data:")
    print(sales.shape)
    print()
    
    print("Size of the critics data:")
    print(critics.shape)
    print()
    
    print("Size of the resulting merge:")
    print(output.shape)
    print()
    
    # We cut out repeated / irrelevant columns
    output = output.drop(CUT_COLUMNS, axis=1)
    
    # We cut out all columns containing null values
    output = output.loc[:, output.all()]
    
    print("Size of the reduced version:")
    print(output.shape)
    
    output.to_csv(output_filename)
    print("Data saved in file: " + output_filename)

if __name__ == "__main__":
    merge(CRITICS_FILENAME, SALES_FILENAME, OUTPUT_FILENAME)