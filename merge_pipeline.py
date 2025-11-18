""" Goal: produce a feeder × month-hour feature matrix with:
- Load shape features (kWh per GP)
- EV adoption (via ZIP)
- Temperature (via ZIP or grid)
- ICA / feeder metadata (line rating, %res, %ind, %com, congestion Y/N)
"""

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
def load_calmac_load_shapes():
    """
    Load CALMAC hourly load shapes.

    Expects a CSV with at least: gp, date, hour, kwh
    """
    print("Loading residential electric load shapes from CALMAC/Res_GP_Elec_2024.csv...")
    load_data = pd.read_csv("CALMAC/Res_GP_Elec_2024.csv")
    return load_data

#1. Load spatial data
def load_climate_zones():
    """Load CEC building climate zones shapefile."""
    print("Loading climate zones...")
    climate_zones = gpd.read_file("CALMAC/Building_Climate_Zones.shp")
    return climate_zones

def load_zip_polygons():
    """Load ZIP code polygons shapefile."""
    print("Loading ZIP polygons...")
    zips = gpd.read_file("zip_codes/zip_poly.shp")
    return zips

    
def load_feeders():
    """
    Load feeder shapefile.

    TODO: update the path and column names to match your actual feeder data.
    Assumes:
      - geometry: LineString or MultiLineString
      - a unique feeder ID column, e.g. 'feeder_id'
    """
    print("Loading feeders...")
    feeders = gpd.read_file("ica_data/FeederDetail_Voltage.shp")  
    return feeders

# Map ZIP -> climate zone --> GP list
def map_climate_zones(climate_zones: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add CALMAC climate zone group labels to CEC climate zones.

    cz_groups mapping is from CALMAC documentation.
    """
    print("Mapping climate zones to CALMAC groups...")
    cz_groups = {
        1: "Coastal", 3: "Coastal", 5: "Coastal",
        2: "Inland", 4: "Inland",
        11: "North Central Valley", 12: "North Central Valley",
        13: "South Central Valley",
    }

    climate_zones["BZone"] = climate_zones["BZone"].astype(int)
    climate_zones["cz_groups"] = climate_zones["BZone"].map(cz_groups)
    return climate_zones


def process_zip_climate_mapping(zips: gpd.GeoDataFrame, climate_zones: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Assign each ZIP a climate zone group based on centroid location.
    Returns a GeoDataFrame with ZIP geometry and cz_groups.
    """
    print("Processing ZIP → climate group mapping...")
    zips = zips.to_crs(climate_zones.crs)
    zips_centroids = zips.copy()
    zips_centroids["geometry"] = zips_centroids.geometry.centroid

    joined = gpd.sjoin(
        zips_centroids,
        climate_zones[["cz_groups", "geometry"]],
        how="left",
        predicate="within"
    )
    zips["cz_groups"] = joined["cz_groups"]
    zips_climate = zips[zips["cz_groups"].notna()].copy()
    return zips_climate  # has ZIP_CODE, geometry, cz_groups, etc.


def load_calmac_characteristics():
    """
    Load CALMAC residential and non-residential GP characteristics and
    return a combined DataFrame with 'gp' and 'seg_cz' columns.
    """
    print("Loading CALMAC characteristics...")
    res_chars = pd.read_csv("CALMAC/res_characteristics.csv")
    nonres_chars = pd.read_csv("CALMAC/nonres_characteristics.csv")

    res_chars["type"] = "residential"
    nonres_chars["type"] = "nonresidential"

    gps_all = pd.concat([res_chars, nonres_chars], ignore_index=True)
    return gps_all


def zip_gp_lookup(zips_climate: gpd.GeoDataFrame, gps_all: pd.DataFrame) -> pd.DataFrame:
    """
    Build a ZIP → GP lookup table no geometry.

    Steps:
    - Group GPs by CALMAC climate zone label (seg_cz)
    - Merge onto ZIPs by cz_groups (must match seg_cz labels: e.g. 'Coastal')
    - Explode GP lists so we get one row per ZIP–GP pair
    """
    print("Building ZIP → GP lookup (no geometry)...")

    # Group GPs by CALMAC segment climate zone
    gps_by_zone = gps_all.groupby("seg_cz")["gp"].apply(list).reset_index()
    gps_by_zone.rename(columns={"gp": "gp_list"}, inplace=True)

    # zips_climate has a 'cz_groups' column,  match  to 'seg_cz'
    zips_small = zips_climate[["ZIP_CODE", "cz_groups"]].drop_duplicates()

    zip_gp = zips_small.merge(
        gps_by_zone,
        left_on="cz_groups",
        right_on="seg_cz",
        how="left"
    )

    zip_gp = zip_gp.drop(columns=["seg_cz"])

    # Explode gp_list to one row per ZIP–GP
    zip_gp = zip_gp.explode("gp_list").rename(columns={"gp_list": "gp"})

    print(f"ZIP–GP pairs: {len(zip_gp)}")
    return zip_gp  

#3. Aggregate Load Shapes gp x month x hour
def aggregate_load_shapes():
    print("Loading CALMAC load shapes...")
    load_data = load_calmac_load_shapes()

    # Ensure date is datetime
    load_data["date"] = pd.to_datetime(load_data["date"])

    # Filter to May–October
    load_data = load_data[load_data["date"].dt.month.isin([5, 6, 7, 8, 9, 10])].copy()
    load_data["month"] = load_data["date"].dt.month

    # Aggregate to month-hour-gp
    load_month_hour = (
        load_data
        .groupby(["gp", "month", "hour"], as_index=False)
        .agg({"kwh": "mean"})  # or "sum" depending on how you want to use it
    )

    print(
        f"Aggregated load rows: {len(load_month_hour)} "
        f"({load_month_hour['gp'].nunique()} GPs, "
        f"{load_month_hour['month'].nunique()} months, "
        f"{load_month_hour['hour'].nunique()} hours)"
    )
    return load_month_hour  # gp, month, hour, kwh

# 4. Map feeders to zips

def feeder_zips_map(feeders: gpd.GeoDataFrame, zips_climate: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Map each feeder to a ZIP using a spatial join."""
    print("Mapping feeders to ZIPs...")
    print("Mapping feeders → ZIPs...")

    # Ensure same CRS
    feeders = feeders.to_crs(zips_climate.crs)

    # Use centroid for assignment
    feeders = feeders.copy()
    feeders["centroid"] = feeders.geometry.centroid
    feeders_centroids = feeders.set_geometry("centroid")

    feeder_zip = gpd.sjoin(
        feeders_centroids,
        zips_climate[["ZIP_CODE", "geometry"]],
        how="left",
        predicate="within"
    )
    feeder_zip = feeder_zip[["feederid", "ZIP_CODE"]].dropna(subset=["ZIP_CODE"])
    #only one zip per feeder
    feeder_zip_map = (
        feeder_zip.drop_duplicates(subset=["feederid"]).reset_index(drop=True)
    )

    print(f"Unique feeders mapped: {feeder_zip_map['feederid'].nunique()}")
    return feeder_zip_map

# 5. Pivot wide
def build_feeder_gp(zip_gp: pd.DataFrame, load_month_hour: pd.DataFrame, feeder_zip_map: pd.DataFrame) -> pd.DataFrame:
    """
    Build feeder-level load features. Final shape: one row per feederid, ZIP, month, hour)
    """
    print("Building feeder × month-hour load features...")
    # keep only zips with feeders
    zips_for_feeders = feeder_zip_map["ZIP_CODE"].unique()
    zip_gp_sub = zip_gp[zip_gp["ZIP_CODE"].isin(zips_for_feeders)].copy()

    # Join ZIP → feeder to get feeder–ZIP–GP
    feeder_zip_gp = feeder_zip_map.merge(
        zip_gp_sub,
        on="ZIP_CODE",
        how="left"
    )
    
    # feeder-gp pairs
    feeder_gp = (
        feeder_zip_gp[["feederid", "gp"]].dropna(subset=["gp"]).drop_duplicates()
    )
    print(f"Feeder-GP pairs: {len(feeder_gp)}")

    #join feeder-GP with loads
    feeder_gp_month_hour = feeder_gp.merge(
        load_month_hour,
        on="gp",
        how="left"
    )
    #pivot GP to columns: one row per feeder-month-hour
    feeder_wide = feeder_gp_month_hour.pivot_table(
        index=["feederid", "month", "hour"],
        columns="gp",
        values="kwh",
        aggfunc="mean",
        fill_value=0.0,
    ).reset_index()

    # flatten GP columns, rename kwh_gp
    feeder_wide.columns = [
        f"kwh_{c}" if isinstance(c, str) and not c in {"feederid", "month", "hour"} else c
        for c in feeder_wide.columns
    ]
    #merge Zip_code back in
    feeder_wide = feeder_wide.merge(
        feeder_zip_map,
        on="feederid",
        how="left"
    )

    print(f"Feeder-wide feature rows: {len(feeder_wide)} "
          f"and {len(feeder_wide.columns)} columns")
    return feeder_wide

def main():
    # 1. Load  climate + ZIP
    climate_zones = load_climate_zones()
    climate_zones = map_climate_zones(climate_zones)

    zips = load_zip_polygons()
    zips_climate = process_zip_climate_mapping(zips, climate_zones)

    # 2. CALMAC GPs + ZIP to GP mapping
    gps_all = load_calmac_characteristics()
    zip_gp = zip_gp_lookup(zips_climate, gps_all)

    # 3. Aggregate CALMAC load shapes to gp × month × hour (May–Oct)
    load_month_hour = aggregate_load_shapes()

    # 4. Load feeders + map to ZIPs
    feeders = load_feeders()
    feeder_zip_map = feeder_zips_map(feeders, zips_climate)

    # 5. Build feeder × month-hour × GP load matrix
    feeder_features = build_feeder_gp(zip_gp, load_month_hour, feeder_zip_map)

    
    feeder_features.to_csv("outputs/feeder_load_features.csv", index=False)
    print("Saved feeder_load_features.csv")

    return feeder_features


if __name__ == "__main__":
    feeder_features = main()

    