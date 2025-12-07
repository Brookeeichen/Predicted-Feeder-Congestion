import pandas as pd

# --- Configuration paths ---
FINAL_PATH = "outputs/feeder_load_features.csv"
FEEDER_CHARS_PATH = "ica_data/feeder_characteristics.csv"
OUTPUT_PATH = "outputs/feeder_load_features.csv"   



def normalize_feederid(series):
    """
    Normalizes feederid values by removing leading zeros
    """
    s = series.astype(str).str.strip()
    s_nozero = s.str.lstrip("0")
    return s_nozero.replace({"": "0"})


def main():
    print("Loading final feeder features...")
    ff = pd.read_csv(FINAL_PATH, low_memory=False)

    print("Loading feeder characteristics...")
    fc = pd.read_csv(FEEDER_CHARS_PATH, low_memory=False)

    # Standardizes feeder characteristics column names to lowercase and stripped format
    fc.columns = fc.columns.str.strip().str.lower()

    # Adds normalized feederid columns to both datasets
    ff["feederid_norm"] = normalize_feederid(ff["feederid"])
    fc["feederid_norm"] = normalize_feederid(fc["feederid"])

    print("Unique feederids in final df:", ff["feederid_norm"].nunique())
    print("Unique feederids in feeder_chars:", fc["feederid_norm"].nunique())

    # Metadata columns expected from feeder_characteristics.csv
    meta_cols = [
        "nominal voltage (kv)",
        "redacted data",
        "residential customer count",
        "commercial customer count",
        "industrial customer count",
        "agricultural customer count",
        "other customers",
        "existing distributed generation (kw)",
        "queued distributed generation (kw)",
        "total distributed generation (kw)",
        "voltage",
    ]

    # Identifies metadata columns missing from the feeder characteristics file
    missing_in_fc = [c for c in meta_cols if c not in fc.columns]
    if missing_in_fc:
        print("\nWARNING: These metadata columns are not in feeder_characteristics.csv:")
        print(missing_in_fc)
    else:
        print("\nAll metadata columns present in feeder_characteristics.csv.")

    # Removes stale metadata columns from the final dataframe to avoid suffixes during merge
    print("\nDropping old metadata columns from final df...")
    ff_before_rows = len(ff)
    ff = ff.drop(columns=meta_cols, errors="ignore")
    print(f"Rows remain unchanged after drop: {len(ff)} (was {ff_before_rows})")

    # Extracts only normalized feederid and relevant metadata columns
    merge_cols = ["feederid_norm"] + meta_cols
    fc_meta = fc[merge_cols].drop_duplicates(subset=["feederid_norm"])

    print("\nMerging fresh metadata onto final df...")
    ff = ff.merge(fc_meta, on="feederid_norm", how="left")

    # Removes temporary normalized feederid column
    ff = ff.drop(columns=["feederid_norm"])

    print("\nDiagnostics after re-merge:")
    print("Non-NA counts for metadata columns:")
    print(ff[meta_cols].notna().sum())

    print("\nFraction NA for metadata columns:")
    print(ff[meta_cols].isna().mean())

    print(f"\nSaving fixed df to: {OUTPUT_PATH}")
    ff.to_csv(OUTPUT_PATH, index=False)
    print("Done.")


if __name__ == "__main__":
    main()