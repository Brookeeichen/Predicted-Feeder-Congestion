import os

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

FINAL_DF = "outputs/feeder_features_cleaned.csv"
FEEDER_SHP = "ica_data/FeederDetail_Voltage.shp"


def normalize_feederid(series: pd.Series) -> pd.Series:
    """
    Normalizes feeder IDs to a common format:
      - Casts to string
      - Strips surrounding whitespace
      - Strips leading zeros ("001234" -> "1234")
      - Replaces empty strings with "0"
    """
    s = series.astype(str).str.strip()
    s_nozero = s.str.lstrip("0")
    return s_nozero.replace({"": "0"})


def main():

    os.makedirs("outputs/validation", exist_ok=True)

    print("\n=== Loading data ===")
    ff = pd.read_csv(FINAL_DF, low_memory=False)
    feeders = gpd.read_file(FEEDER_SHP)

    # Normalize feeder IDs consistently
    ff["feederid_norm"] = normalize_feederid(ff["feederid"])
    feeders["feederid_norm"] = normalize_feederid(feeders["feederid"])

    # -------------------------------
    # 1. BASIC STRUCTURAL VALIDATION
    # -------------------------------
    print("\n=== Structural validation ===")
    print("Rows in final df:", len(ff))
    print("Columns in final df:", ff.shape[1])
    print("Unique feeders (raw):", ff["feederid"].nunique())
    print("Unique feeders (normalized):", ff["feederid_norm"].nunique())
    print("Unique feeder geometries (normalized):", feeders["feederid_norm"].nunique())
    print("\nFirst few rows of final df:")
    print(ff.head())

    # Simple feeder ID intersection check
    ff_ids = set(ff["feederid_norm"])
    shp_ids = set(feeders["feederid_norm"])
    print("\nFeeder ID universe check (normalized):")
    print("  Final df feeders:", len(ff_ids))
    print("  Shapefile feeders:", len(shp_ids))
    print("  Intersection:", len(ff_ids & shp_ids))
    print("  In final df but not in shapefile:", len(ff_ids - shp_ids))

    # -------------------------------
    # 2. Missingness checks
    # -------------------------------
    print("\n=== Missingness summary ===")
    missing = ff.isna().mean().sort_values(ascending=False)
    print(missing)

    # Time-pattern missingness for ICA
    if "mean_ica_sg" in ff.columns:
        print("\nComputing missingness matrix for ICA...")
        na_by_time = (
            ff.assign(is_na=ff["mean_ica_sg"].isna())
              .groupby(["month", "hour"])["is_na"]
              .mean()
              .reset_index()
        )

        pivot = na_by_time.pivot(index="hour", columns="month", values="is_na")

        plt.figure(figsize=(8, 6))
        sns.heatmap(pivot, cmap="viridis_r")
        plt.title("Fraction of NA ICA Values by Month × Hour")
        plt.xlabel("Month")
        plt.ylabel("Hour")
        plt.tight_layout()
        plt.savefig("outputs/validation/missingness_heatmap.png", dpi=200)
        plt.close()

    # -------------------------------
    # 3. ICA Distribution
    # -------------------------------
    if "mean_ica_sg" in ff.columns:
        print("\n=== ICA distribution stats ===")
        print(ff["mean_ica_sg"].describe())

        plt.figure(figsize=(8, 5))
        sns.histplot(ff["mean_ica_sg"].dropna(), bins=50, kde=True)
        plt.xlabel("mean_ica_sg")
        plt.title("Distribution of ICA (Generation Headroom)")
        plt.tight_layout()
        plt.savefig("outputs/validation/hist_mean_ica_sg.png", dpi=200)
        plt.close()

    # -------------------------------
    # 4. Spatial validation
    # -------------------------------
    print("\n=== Spatial validation ===")

    # Aggregate ICA to one value per normalized feeder ID
    if "mean_ica_sg" in ff.columns:
        ff_unique = (
            ff.groupby("feederid_norm", as_index=False)["mean_ica_sg"]
              .mean()
        )
    else:
        ff_unique = ff[["feederid_norm"]].drop_duplicates()
        ff_unique["mean_ica_sg"] = np.nan

    # Merge feeder geometries with aggregated ICA
    feeders_merged = feeders.merge(
        ff_unique,
        on="feederid_norm",
        how="left"
    )

    # Map 1 — all feeders (geometry only)
    ax = feeders.plot(figsize=(8, 10), color="lightgray", linewidth=0.5)
    plt.title("All PG&E Feeders (from shapefile)")
    plt.tight_layout()
    plt.savefig("outputs/validation/map_all_feeders.png", dpi=200)
    plt.close()

    # Map 2 — feeders included in final df, colored by ICA
    feeders_merged.plot(
        figsize=(8, 10),
        column="mean_ica_sg",
        cmap="plasma",
        legend=True,
        missing_kwds={"color": "lightgray", "label": "No ICA in final df"},
    )
    plt.title("Feeders Colored by Average ICA (NA in gray)")
    plt.tight_layout()
    plt.savefig("outputs/validation/map_feeders_ica.png", dpi=200)
    plt.close()

    print("\nValidation figures saved to outputs/validation/")
    print("Done.")


if __name__ == "__main__":
    main()