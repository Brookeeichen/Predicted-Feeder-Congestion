""" Goal: produce a feeder × month-hour feature matrix with:
- Load shape features (kWh per GP)
- EV adoption (via ZIP)
- Temperature (via ZIP or grid)
- ICA / feeder metadata (line rating, %res, %ind, %com, congestion Y/N)
"""

import pandas as pd
import geopandas as gpd
import glob
import os
import zipfile
from io import BytesIO

def load_feeder_characteristics(path="ica_data/feeder_characteristics.csv"):
    """
    Load feeder-level data, including DER capacity, customer mix, etc. and drop unneeded columns.

    """
    print("Loading feeder characteristics...")

    df = pd.read_csv(path)

    df.columns = df.columns.str.strip().str.lower()

    if "feederid" not in df.columns:
        raise ValueError("feeder_characteristics must contain a 'Feeder ID' column.")
    drop_cols = [
        "feeder_name",
        "substation name",
        "division",
        "last_update_on_map",
        "publish",
        "objectid",
        "nominal_voltage_kV", #this would probably be a strong predictor, but removing it for the transferrability of our model
        "redacted_data",
        "shape__length",

    ]
    drop_cols = [c for c in drop_cols if c in df.columns]
    df = df.drop(columns=drop_cols)

    
    df["feederid"] = (
        df["feederid"]
        .astype(str)
        .str.strip()
    )

    return df

def process_line_zips(
    input_dir, loading_scenario, ica_col, debug: bool = False,
):
    """
    Stream through PG&E division ZIPs and compute feeder-level
    ICA data

    Steps (per feeder ZIP inside each division ZIP):
      - Read inner CSV into DF
      - Filter to Loading_Scenario == 90
      - Drop  Monthly_ICA_SG columns
      - group by month-hour and gen/load
      - Take min() of `ica_col` across line sections
      - Store one row per feeder: feederid, Min_ICA_OF

    Returns
    -------
    feeder_ica : pd.DataFrame
        Columns: ['feederid', 'Min_ICA_OF']
    """
    records = []
    #This step extracts and opens the files within the PG&E division zip files from GRIP portal
    division_zips = glob.glob(os.path.join(input_dir, "*.zip"))
    if not division_zips: 
        print(f"[ICA] No division ZIPs found in {input_dir}")
        return pd.DataFrame(columns=["feederid", "month", "hour", "loadorgen", "min_ica_of"])
    if debug: #only process first division .zip in debug mode
        division_zips = division_zips[:1]

    for div_zip_path in division_zips:
        print(f"[ICA] Processing division zip: {os.path.basename(div_zip_path)}")
        with zipfile.ZipFile(div_zip_path, "r") as div_zip:
            for inner_name in div_zip.namelist():
                # only process inner ZIPs 
                if not inner_name.lower().endswith(".zip"):
                    continue

                # get feederid from inner ZIP name (zip files hold line sections, title = feederID)
                feeder_id_raw = os.path.splitext(os.path.basename(inner_name))[0]
                # Handle names like 'GICA_102530401' - GICA = Generation data, LICA = Load data
                parts = feeder_id_raw.split("_")
                if len(parts) > 1:
                    # Take  part after  underscore since that = feederID
                    feederid = parts[-1]
                else:
                    feederid = feeder_id_raw

                try:
                    inner_bytes = div_zip.read(inner_name)
                except KeyError:
                    print(f"[ICA] Warning: could not read {inner_name} in {div_zip_path}")
                    continue

                with zipfile.ZipFile(BytesIO(inner_bytes), "r") as feeder_zip:
                    csv_members = [
                        m for m in feeder_zip.namelist()
                        if m.lower().endswith(".csv")
                    ]
                    if not csv_members:
                        print(f"[ICA] Warning: no CSV in {inner_name}")
                        continue

                    csv_name = csv_members[0]
                    with feeder_zip.open(csv_name) as src:
                        df = pd.read_csv(src)
                #Make sure whichever column used to determine headroom is present
                df.columns = df.columns.str.strip().str.lower()
                if ica_col not in df.columns:
                    print(f"[ICA] Warning: '{ica_col}' not found in {csv_name}; skipping this file")
                    continue
                #Ensure all other relevant columns are present. "loadorgen" used to identify whether the row represents generation or load headroom
                required_group_cols = ["month", "hour", "loadorgen"]
                missing = [c for c in required_group_cols if c not in df.columns]
                if missing:
                    print(f"[ICA] Warning: missing {missing} in {csv_name}; skipping this file")
                    continue
                # filter by loading scenario. This code filters for 90th pctl loading scenario.    
                if "loading_scenario" in df.columns and loading_scenario is not None:
                    df = df[df["loading_scenario"] == loading_scenario].copy()
                #drop ica_sg because we care about operational flexibility constraints (ica_of)
                if "monthly_ica_sg" in df.columns:
                    df = df.drop(columns=["monthly_ica_sg"])
                # turn the feederid variable into a column, attach to every row inside zip file
                df["feederid"] = feederid
                df[ica_col] = pd.to_numeric(df[ica_col], errors="coerce") #convert to numeric, assign NA if not
                df = df.dropna(subset=[ica_col]) #drop rows where ica_of is NA
                if df.empty:
                    continue
                group_cols = ["feederid", "month", "hour", "loadorgen"]
                #because each feeder has many line sections, we take the min of ica_of across line sections 
                grouped = (
                    df.groupby(group_cols, as_index=False)[ica_col]
                    .min()
                    .rename(columns={ica_col: "min_ica_of"})
                )
                #constucting combined DF from each feeder csv, keep only grouped columns + min_ica_of
                records.append(grouped)

    if not records:
        raise ValueError("[ICA] No ICA values computed from division ZIPs.")

    feeder_ica = pd.concat(records, ignore_index=True)
    #Some feeders have multiple files, group again
    feeder_ica = (feeder_ica.groupby(["feederid", "month", "hour", "loadorgen"], as_index=False).agg({"min_ica_of": "min"}))

    print(
        f"[ICA] Computed feeder-level ICA for "
        f"{feeder_ica['feederid'].nunique()} feeders "
        f"across {feeder_ica['loadorgen'].unique().tolist()}."
    )
    #columns: "feederid", "month", "hour", "loadorgen", "min_ica_of" 
    return feeder_ica

def load_calmac_load_shapes():
    """
    Load CALMAC hourly load shapes. 160 total residential load shapes, 40 per climate zone.

    CSV shoudl contain [gp (load shape identifier), date, hour, kwh]
    """
    print("Loading residential electric load shapes from CALMAC/Res_GP_Elec_2024.csv...")
    load_data = pd.read_csv("CALMAC/Res_GP_Elec_2024.csv")
    return load_data


# Load spatial data
def load_climate_zones():
    """Load CEC building climate zones shapefile. 
    This is used to spacially map load shapes (GPs) and assign to correct ZIP code"""
    print("Loading climate zones...")
    climate_zones = gpd.read_file("CALMAC/Building_Climate_Zones.shp")
    return climate_zones

def load_zip_polygons():
    """Load ZIP code polygons shapefile. Used to determine which ZIPs/Climate zones belong together"""
    print("Loading ZIP polygons...")
    zips = gpd.read_file("zip_codes/zip_poly.shp")
    return zips

    
def load_feeder_shapes():
    """
    Load feeder shapefile. Used to Join with ZIP codes.
    """
    print("Loading feeders...")
    feeders = gpd.read_file("ica_data/FeederDetail_Voltage.shp")  
    return feeders

# Map ZIP -> climate zone --> GP list
def map_climate_zones(climate_zones: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    CALMAC documentation says which Climate Zones correspond to their 4 PG&E service territories.
    Add CALMAC climate zone (extracted from (GP)) labels to CEC climate zones.
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
    Assign each ZIP a climate zone group based on the CZ that has the largest overlap area.
    Returns a GeoDataFrame with ZIP geometry and cz_groups.
    """
    print("Processing ZIP → climate group mapping (majority-area overlay)...")
    # Ensure both layers are in the same CRS for area calculations
    zips = zips.to_crs(climate_zones.crs)

    # Intersect ZIP polygons with climate zone polygons to get overlap pieces
    zip_cz_overlap = gpd.overlay(
        zips,
        climate_zones[["cz_groups", "geometry"]],
        how="intersection",
    )

    # Compute area of each overlap piece
    zip_cz_overlap["overlap_area"] = zip_cz_overlap.geometry.area

    # For each ZIP, find the climate group with the largest overlapping area
    # Assumes ZIP_CODE uniquely identifies each ZIP polygon
    idx = zip_cz_overlap.groupby("ZIP_CODE")["overlap_area"].idxmax()
    majority = zip_cz_overlap.loc[idx, ["ZIP_CODE", "cz_groups"]]

    # Join the primary climate group back onto the original ZIP geometries
    zips_climate = zips.merge(majority, on="ZIP_CODE", how="left")
    zips_climate = zips_climate[zips_climate["cz_groups"].notna()].copy()
    return zips_climate  # has ZIP_CODE, geometry, cz_groups, etc.


def load_calmac_characteristics():
    """
    Load CALMAC residential and non-residential GP characteristics and
    return a combined DataFrame with 'gp' and climate zone columns.
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

    # Group GPs by CALMAC segment climate zone, create list of relevant GPs per CZ
    gps_by_zone = gps_all.groupby("seg_cz")["gp"].apply(list).reset_index()
    gps_by_zone.rename(columns={"gp": "gp_list"}, inplace=True)

    # keep only necessary columns
    zips_small = zips_climate[["ZIP_CODE", "cz_groups"]].drop_duplicates()
    # zips_climate has a 'cz_groups' column,  match  to 'seg_cz'
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

# Aggregate KWH consumed across gp x month x hour
def aggregate_load_shapes():
    print("Loading CALMAC load shapes...")
    load_data = load_calmac_load_shapes()

    # Ensure date is datetime
    load_data["date"] = pd.to_datetime(load_data["date"])

    load_data["month"] = load_data["date"].dt.month

    # Aggregate to month-hour-gp
    load_month_hour = (
        load_data
        .groupby(["gp", "month", "hour"], as_index=False)
        .agg({"kwh": "mean"})  
    )

    print(
        f"Aggregated load rows: {len(load_month_hour)} "
        f"({load_month_hour['gp'].nunique()} GPs, "
        f"{load_month_hour['month'].nunique()} months, "
        f"{load_month_hour['hour'].nunique()} hours)"
    )
    return load_month_hour  # columns: gp, month, hour, kwh

# Map feeders to zips

def feeder_zips_map(feeders: gpd.GeoDataFrame, zips_climate: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Map each feeder to a ZIP using majority-area overlay."""
    print("Mapping feeders to ZIPs (majority-area overlay)...")

    # Ensure same CRS
    feeders = feeders.to_crs(zips_climate.crs)

    # Intersect feeder polygons with ZIP polygons to get overlap pieces
    feeder_zip_overlap = gpd.overlay(
        feeders[["feederid", "geometry"]],
        zips_climate[["ZIP_CODE", "geometry"]],
        how="intersection",
    )

    # Compute area of each overlap piece
    feeder_zip_overlap["overlap_area"] = feeder_zip_overlap.geometry.area

    # For each feeder, find the ZIP with the largest overlapping area
    idx = feeder_zip_overlap.groupby("feederid")["overlap_area"].idxmax()
    feeder_zip = feeder_zip_overlap.loc[idx, ["feederid", "ZIP_CODE"]]

    # Only one ZIP per feeder 
    feeder_zip_map = feeder_zip.reset_index(drop=True)

    print(f"Unique feeders mapped: {feeder_zip_map['feederid'].nunique()}")
    return feeder_zip_map

# Pivot wide - necessary to keep DF from exploding
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
        aggfunc="mean", #shouldn't be duplicates, but pivot_wide requires an aggfunc
        #GPs that don't belong to a feeder will be "NaN"
    ).reset_index()

    # flatten GP columns, rename kwh_gp
    feeder_wide.columns = [
        f"kwh_{c}" if isinstance(c, str) and not c in {"feederid", "month", "hour"} else c
        for c in feeder_wide.columns
    ]

    # Sum across all kwh_* columns to get a single load shape per feeder-month-hour
    gp_cols = [c for c in feeder_wide.columns if isinstance(c, str) and c.startswith("kwh_")]
    feeder_wide["load"] = feeder_wide[gp_cols].sum(axis=1)

    # Keep only aggregated load plus keys
    feeder_wide = feeder_wide[["feederid", "month", "hour", "load"]]

    # Merge ZIP_CODE back in
    feeder_wide = feeder_wide.merge(
        feeder_zip_map,
        on="feederid",
        how="left"
    )

    print(f"Feeder-wide feature rows: {len(feeder_wide)} with aggregated load shape")
    return feeder_wide

def load_ev_data(ev_path: str = "EV_Pop_Growth_23_24.csv") -> pd.DataFrame:
    """
    Load EV adoption data and normalize ZIP code.

    Expects a column 'Zip Code' in the CSV.
    Returns a DataFrame with a standardized 'ZIP_CODE' column.
    """
    print("Loading EV data...")
    ev = pd.read_csv(ev_path)

    # Normalize ZIP codes to 5-char str
    ev["Zip Code"] = (
        ev["Zip Code"]
        .astype(str)
        .str.strip()
        .str.zfill(5)
    )

    # Drop duplicate ZIPs 
    ev_unique = ev.drop_duplicates(subset=["Zip Code"]).copy()

    # Rename to match feeder_features
    ev_unique = ev_unique.rename(columns={"Zip Code": "ZIP_CODE"})

    print(f"Unique ZIPs in EV data: {ev_unique['ZIP_CODE'].nunique()}")
    return ev_unique
def attach_ev_to_feeders(feeder_features: pd.DataFrame, ev_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge ZIP-level EV data onto feeder_features (feeder × month × hour).

    - feeder_features: must have 'ZIP_CODE'
    - ev_df: must have 'ZIP_CODE' + EV columns

    Result: same number of rows as feeder_features, with EV columns added.
    """
    print("Merging EV data onto feeder_features...")

    # Normalize ZIP format in feeder_features 
    ff = feeder_features.copy()
    ff["ZIP_CODE"] = (
        ff["ZIP_CODE"]
        .astype(str)
        .str.strip()
        .str.zfill(5)
    )

    merged = ff.merge(
        ev_df,
        how="left",
        on="ZIP_CODE",
        suffixes=("", "_EV")  # EV side gets suffix if there are name conflicts
    )

    print(f"Rows before EV merge: {len(ff):,}")
    print(f"Rows after EV merge:  {len(merged):,}")
    return merged

def main(debug: bool = False):
    # Load  climate + ZIP
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
    feeders = load_feeder_shapes()
    feeder_zip_map = feeder_zips_map(feeders, zips_climate)

    # 5. Build feeder × month-hour × GP load matrix
    feeder_features = build_feeder_gp(zip_gp, load_month_hour, feeder_zip_map)
    #6. Load EV data and merge onto feeder_features
    ev_df = load_ev_data()
    feeder_features = attach_ev_to_feeders(feeder_features, ev_df)

    feeder_ica = process_line_zips(
        input_dir="ica_data/raw_zips",
        loading_scenario=90,
        ica_col="hourly_ica_of",
        debug=debug,
    )

    # This will duplicate rows, one for load and one for gen
    feeder_features = feeder_features.merge(
        feeder_ica,
        on=["feederid", "month", "hour"],
        how="left",
    )
    
    # 7. Load feeder-level metadata and merge onto every feeder × month × hour row
    feeder_chars = load_feeder_characteristics()

    # Ensure feederid is a normalized string key on both sides before merging
    feeder_features["feederid"] = (
        feeder_features["feederid"].astype(str).str.strip()
    )
    feeder_chars["feederid"] = (
        feeder_chars["feederid"].astype(str).str.strip()
    )

    feeder_features = feeder_features.merge(
        feeder_chars,
        on="feederid",
        how="left"
    )

    print("Loading weather data...")
    weather = pd.read_csv("weather/2024_weather_cleaned.csv")

    # Normalize column names
    weather.rename(columns={
        "Month": "month",
        "Hour": "hour"
    }, inplace=True)
    # Convert weather hour column from 100,200,... to 1,2,... or 0–23 range
    weather["hour"] = (weather["hour"] // 100).astype(int)
    # Fix 24 → hour 0
    weather.loc[weather["hour"] == 24, "hour"] = 0

    # Normalize ZIP + month again after adjusting hour
    weather["ZIP_CODE"] = weather["ZIP_CODE"].astype(str).str.zfill(5)
    weather["month"] = weather["month"].astype(int)
    # Ensure types match for merge keys
    weather["ZIP_CODE"] = weather["ZIP_CODE"].astype(str).str.zfill(5)
    weather["month"] = weather["month"].astype(int)
    weather["hour"] = weather["hour"].astype(int)

    print("Merging weather data...")
    feeder_features = feeder_features.merge(
        weather,
        on=["ZIP_CODE", "month", "hour"],
        how="left"
    )

    print("Weather coverage:", weather["ZIP_CODE"].nunique(), "ZIPs in weather file")
    print("After merge, rows with weather:", feeder_features["Sol Rad (Ly/day)"].notna().sum())

    # 8. Save in repo
    os.makedirs("outputs", exist_ok=True)
    feeder_features.to_csv("outputs/feeder_load_features.csv", index=False)
    print("Saved outputs/feeder_load_features.csv")

    return feeder_features   
if __name__ == "__main__":
    feeder_features = main(debug=False) #debug = TRUE tests pipeline with less data