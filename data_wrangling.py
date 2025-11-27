"""
Data Wrangling Script for Predicted Feeder Congestion
Extracted from Data Wrangling.ipynb

This script processes climate zones, zip codes, and CALMAC load shape data
to create a merged dataset for feeder congestion analysis.
"""

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
# Import load data processing functions from the load_data_loader module
# These functions handle loading and merging of CALMAC hourly load shape data
from load_data_loader import load_calmac_load_shapes, merge_load_data

def load_climate_zones():
    """Load shapefile of CEC climate zones"""
    print("Loading climate zones...")
    climate_zones = gpd.read_file("CALMAC/Building_Climate_Zones.shp")
    return climate_zones

def load_zip_codes():
    """Load zip code shapefile"""
    print("Loading zip codes...")
    zips = gpd.read_file("zip_codes/zip_poly.shp")
    return zips

def map_climate_zones(climate_zones):
    """Map climate zones to CALMAC territories"""
    # Mapping climate zones to CALMAC territories, from CALMAC data description
    cz_groups = {
        1: "Coastal", 3: "Coastal", 5: "Coastal",
        2: "Inland", 4: "Inland",
        11: "North Central Valley", 12: "North Central Valley",
        13: "South Central Valley"
    }
    
    climate_zones["BZone"] = climate_zones["BZone"].astype(int)
    climate_zones["cz_groups"] = climate_zones["BZone"].map(cz_groups)  # BZone is the CEC column with numeric climate zones
    
    return climate_zones

def process_zip_climate_mapping(zips, climate_zones):
    """Process zip code to climate zone mapping"""
    print("Processing zip code to climate zone mapping...")
    
    # Ensure coordinate systems match
    zips = zips.to_crs(climate_zones.crs)
    
    # Find zip centroids to ensure only one climate zone per ZIP
    zips["centroid"] = zips.centroid
    zips_centroids = zips.set_geometry("centroid")
    
    # Spatial join to find climate zones for each zip centroid
    zips_climate = gpd.sjoin(zips_centroids, climate_zones, how="left", predicate="within")
    
    # Filter out zips without climate zone assignments
    zips_climate = zips_climate[zips_climate["cz_groups"].notna()].copy()
    
    return zips_climate

def load_calmac_characteristics():
    """Load CALMAC residential and non-residential characteristics"""
    print("Loading CALMAC characteristics...")
    
    # Load CALMAC characteristics 
    res_chars = pd.read_csv("CALMAC/res_characteristics.csv")
    nonres_chars = pd.read_csv("CALMAC/nonres_characteristics.csv")
    
    # Combine res and nonres GPs, create 'type' column
    res_chars["type"] = "residential"
    nonres_chars["type"] = "nonresidential"
    
    gps_all = pd.concat([res_chars, nonres_chars], ignore_index=True)
    
    return gps_all

def group_by_climate_zone(gps_all):
    """Group GPs by CALMAC climate zone"""
    print("Grouping GPs by climate zone...")
    
    # Group by CALMAC climate zone 
    gps_by_zone = gps_all.groupby("seg_cz")["gp"].apply(list).reset_index()
    gps_by_zone.rename(columns={"gp": "gp_list"}, inplace=True)
    
    return gps_by_zone

def merge_datasets(zips_climate, gps_by_zone):
    """Merge zip codes with climate zone data"""
    print("Merging datasets...")
    
    zips_final = zips_climate.merge(
        gps_by_zone, left_on="cz_groups", right_on="seg_cz", how="left"
    )
    
    # Drop unnecessary columns
    zips_final = zips_final.drop(columns=["PO_NAME", "STATE", "index_right", "BAcerage"])
    
    return zips_final

def expand_load_shapes(zips_final):
    """Expand the dataset to have one row per load shape"""
    print("Expanding load shapes...")
    
    zips_expanded = zips_final.explode("gp_list").reset_index(drop=True)
    zips_expanded.rename(columns={"gp_list": "gp"}, inplace=True)
    
    return zips_expanded

def load_and_merge_load_data(zips_expanded):
    """Load CALMAC load data, aggregate to month-hour per gp, filter to may-october and merge with expanded ZIP data
    Result: one row per ZIP-gp-month-hour"""
    print("Loading CALMAC load shape data...")
    
    # Load the actual hourly load shape data from CALMAC CSV file
    # This contains gp, date, hour, kwh columns for all load profiles
    load_data = load_calmac_load_shapes()

    # ensure date is datetime
    load_data["date"] = pd.to_datetime(load_data["date"])

    # Filter to may-october
    load_data = load_data[(load_data["date"].dt.month >= 5) & (load_data["date"].dt.month <= 10)].copy()
    load_data["month"] = load_data["date"].dt.month
    load_data = (
        load_data.groupby(["gp", "month", "hour"], as_index=False).agg({"kwh": "mean"})
    )
    print(f"Aggregated rows: {len(load_data)}")

    # Merge the load data with expanded ZIP data using gp as the key
    # This creates the final dataset with hourly consumption for each ZIP
    zips_with_loads = zips_expanded.merge(load_data, on="gp",how="left")
    print(f"Final ZIP-gp-month-hour rows: {len(zips_with_loads)}")
    
    return zips_with_loads

#def save_results(zips_final, zips_expanded, zips_with_loads=None):
   # """Save processed datasets"""
   # print("Saving results...")
    
    # Save the base datasets
    #zips_final.to_csv("zip_codes/zips_final.csv", index=False)
    #zips_expanded.to_csv("zip_codes/zips_expanded.csv", index=False)
    
    # Save the load data if it was merged (optional parameter)
    #if zips_with_loads is not None:
       # zips_with_loads.to_csv("zip_codes/zips_with_loads.csv", index=False)
       # print("Load data merged and saved!")  # Confirmation that load data processing completed
    
    print("Files saved successfully!")

def main():
    """Main data wrangling pipeline"""
    print("Starting data wrangling process...")
    
    # Load data
    climate_zones = load_climate_zones()
    zips = load_zip_codes()
    
    # Process climate zones
    climate_zones = map_climate_zones(climate_zones)
    
    # Process zip code mapping
    zips_climate = process_zip_climate_mapping(zips, climate_zones)
    
    # Load and process CALMAC data
    gps_all = load_calmac_characteristics()
    gps_by_zone = group_by_climate_zone(gps_all)
    
    # Merge datasets
    zips_final = merge_datasets(zips_climate, gps_by_zone)
    
    # Expand load shapes
    zips_expanded = expand_load_shapes(zips_final)
    
    # LOAD DATA INTEGRATION - Load and merge hourly CALMAC load shape data
    # This step creates the final zips_with_loads dataset with hourly consumptio
    zips_with_loads = load_and_merge_load_data(zips_expanded)
    
    # Save all processed datasets including the merged load data
    save_results(zips_final, zips_expanded, zips_with_loads)
    
    # Print summary information
    print("\n=== Data Summary ===")
    print(f"Climate zones: {len(climate_zones)}")
    print(f"Zip codes processed: {len(zips_climate)}")
    print(f"Final merged records: {len(zips_final)}")
    print(f"Expanded records (one per load shape): {len(zips_expanded)}")
    
    # Print unique values for verification
    print("\n=== Climate Zone Groups ===")
    print("Unique cz_groups in zips_climate:", zips_climate["cz_groups"].unique())
    print("Unique seg_cz in gps_by_zone:", gps_by_zone["seg_cz"].unique())
    print("Unique BZone in climate_zones:", climate_zones["BZone"].unique())
    print("Unique cz_groups in climate_zones:", climate_zones["cz_groups"].unique())
    
    print("\nData wrangling completed successfully!")
    
    return zips_final, zips_expanded

if __name__ == "__main__":
    zips_final, zips_expanded = main()
    import pandas as pd
    df = pd.read_csv("zip_codes/zips_with_loads.csv")
    print("\nPreview of zips_with_loads.csv:")
    print(df.head())
