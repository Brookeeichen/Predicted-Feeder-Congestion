"""
Explore merging load shape data with zips_final
This script will show you the data structure and merging process step by step
"""

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

def explore_data_merge():
    """Explore the data merging process step by step"""
    
    print("=== EXPLORING DATA MERGE PROCESS ===\n")
    
    # Step 1: Load and process basic data (without full pipeline)
    print("1. Loading basic data...")
    
    # Load climate zones
    climate_zones = gpd.read_file("CALMAC/Building_Climate_Zones.shp")
    
    # Map climate zones
    cz_groups = {
        1: "Coastal", 3: "Coastal", 5: "Coastal",
        2: "Inland", 4: "Inland",
        11: "North Central Valley", 12: "North Central Valley",
        13: "South Central Valley"
    }
    climate_zones["BZone"] = climate_zones["BZone"].astype(int)
    climate_zones["cz_groups"] = climate_zones["BZone"].map(cz_groups)
    
    # Load characteristics
    res_chars = pd.read_csv("CALMAC/res_characteristics.csv")
    nonres_chars = pd.read_csv("CALMAC/nonres_characteristics.csv")
    res_chars["type"] = "residential"
    nonres_chars["type"] = "nonresidential"
    gps_all = pd.concat([res_chars, nonres_chars], ignore_index=True)
    
    # Group by climate zone
    gps_by_zone = gps_all.groupby("seg_cz")["gp"].apply(list).reset_index()
    gps_by_zone.rename(columns={"gp": "gp_list"}, inplace=True)
    
    print(f"   - Climate zones: {len(climate_zones)}")
    print(f"   - GPS (load profiles): {len(gps_all)}")
    print(f"   - GPS by zone groups: {len(gps_by_zone)}")
    
    # Step 2: Create a sample zips_final (without full spatial processing)
    print("\n2. Creating sample zips_final structure...")
    
    # Sample zips_final structure (normally from spatial join)
    sample_zips_final = pd.DataFrame({
        'ZIP_CODE': [90210, 90211, 90212, 94102, 94103],
        'POPULATION': [10000, 15000, 12000, 20000, 18000],
        'cz_groups': ['Coastal', 'Coastal', 'Inland', 'Coastal', 'Coastal'],
        'gp_list': [
            ['1_1_NS_C', '2_1_NS_C'],  # Coastal profiles
            ['1_1_NS_C', '3_1_NS_C'],  # Coastal profiles  
            ['2_1_NS_I', '4_1_NS_I'],  # Inland profiles
            ['1_1_NS_C', '2_1_NS_C'],  # Coastal profiles
            ['1_1_NS_C', '3_1_NS_C']   # Coastal profiles
        ]
    })
    
    print(f"   - Sample zips_final: {len(sample_zips_final)} ZIP codes")
    print(f"   - Columns: {list(sample_zips_final.columns)}")
    print("\n   Sample zips_final:")
    print(sample_zips_final.head())
    
    # Step 3: Load actual load shape data
    print("\n3. Loading load shape data...")
    
    load_data = pd.read_csv("CALMAC/Res_GP_Elec_2024.csv")
    print(f"   - Load data records: {len(load_data):,}")
    print(f"   - Date range: {load_data['date'].min()} to {load_data['date'].max()}")
    print(f"   - Unique load profiles: {load_data['gp'].nunique()}")
    print(f"   - Load data columns: {list(load_data.columns)}")
    
    # Show sample of load data
    print("\n   Sample load data:")
    print(load_data.head(10))
    
    # Step 4: Expand zips_final (explode gp_list)
    print("\n4. Expanding zips_final (one row per load profile)...")
    
    zips_expanded = sample_zips_final.explode("gp_list").reset_index(drop=True)
    zips_expanded.rename(columns={"gp_list": "gp"}, inplace=True)
    
    print(f"   - Expanded records: {len(zips_expanded)}")
    print("\n   Expanded zips_final:")
    print(zips_expanded)
    
    # Step 5: Check overlap between expanded data and load data
    print("\n5. Checking data overlap...")
    
    expanded_gps = set(zips_expanded['gp'].unique())
    load_gps = set(load_data['gp'].unique())
    
    print(f"   - Unique GPs in expanded zips: {len(expanded_gps)}")
    print(f"   - Unique GPs in load data: {len(load_gps)}")
    print(f"   - Overlapping GPs: {len(expanded_gps.intersection(load_gps))}")
    
    missing_gps = expanded_gps - load_gps
    if missing_gps:
        print(f"   - GPs in zips but NOT in load data: {missing_gps}")
    
    extra_gps = load_gps - expanded_gps  
    if extra_gps:
        print(f"   - GPs in load data but NOT in zips: {list(extra_gps)[:10]}... (showing first 10)")
    
    # Step 6: Perform the merge
    print("\n6. Performing merge...")
    
    # Filter load data to only include GPs we have in zips
    filtered_load_data = load_data[load_data['gp'].isin(expanded_gps)]
    print(f"   - Filtered load data records: {len(filtered_load_data):,}")
    
    # Merge
    zips_with_loads = zips_expanded.merge(filtered_load_data, on='gp', how='left')
    
    print(f"   - Final merged records: {len(zips_with_loads):,}")
    print(f"   - Records with load data: {zips_with_loads['kwh'].notna().sum():,}")
    
    # Show sample of merged data
    print("\n   Sample merged data:")
    print(zips_with_loads.head(15))
    
    # Step 7: Summary statistics
    print("\n7. Summary statistics...")
    
    print(f"   - ZIP codes: {zips_with_loads['ZIP_CODE'].nunique()}")
    print(f"   - Load profiles per ZIP: {zips_with_loads.groupby('ZIP_CODE')['gp'].nunique().mean():.1f} average")
    print(f"   - Total kWh (sample): {zips_with_loads['kwh'].sum():,.2f}")
    print(f"   - Date range in merged data: {zips_with_loads['date'].min()} to {zips_with_loads['date'].max()}")
    
    return zips_with_loads

if __name__ == "__main__":
    merged_data = explore_data_merge()
    print("\n=== EXPLORATION COMPLETE ===")
    print("The merged data is now ready for analysis!")
