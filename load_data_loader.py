"""
CALMAC Load Data Loader
Functions to load and process actual load shape data
"""

import pandas as pd
import numpy as np

def load_calmac_load_shapes():
    """
    Load CALMAC residential electric load shape data from Res_GP_Elec_2024.csv
    """
    print("Loading residential electric load shapes...")
    
    # Load the residential load shape data
    load_data = pd.read_csv("CALMAC/Res_GP_Elec_2024.csv")
    
    print(f"Loaded {len(load_data):,} load shape records")
    print(f"Date range: {load_data['date'].min()} to {load_data['date'].max()}")
    print(f"Number of unique load profiles (gp): {load_data['gp'].nunique()}")
    print(f"Hours per day: {load_data.groupby(['gp', 'date']).size().iloc[0]}")
    
    return load_data

def merge_load_data(zips_expanded, load_data):
    """
    Merge the expanded ZIP data with load shape data
    The load data is in long format: gp, date, hour, kwh
    """
    print("Merging load data with ZIP codes...")
    
    # Merge load shape data onto the expanded ZIP dataset
    # This will create a large dataset with all hourly data for each ZIP
    zips_with_loads = zips_expanded.merge(load_data, on='gp', how='left')
    
    print(f"Merged dataset has {len(zips_with_loads):,} records")
    print(f"Records with load data: {zips_with_loads['kwh'].notna().sum():,}")
    
    return zips_with_loads

def process_load_data_format(load_data):
    """
    Process and validate load data format
    """
    # Check for required columns
    required_cols = ['gp']  # Add other required columns
    
    for col in required_cols:
        if col not in load_data.columns:
            raise ValueError(f"Missing required column: {col}")
    
    # Convert load factors to numeric if needed
    # load_cols = [col for col in load_data.columns if col.startswith('hour_')]
    # load_data[load_cols] = load_data[load_cols].apply(pd.to_numeric, errors='coerce')
    
    return load_data
