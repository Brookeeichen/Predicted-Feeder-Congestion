"""
Validation of ICA + CALMAC Inputs
Saves CSV summaries into outputs/validation/.
"""

import os
import zipfile
import pandas as pd
import glob
from io import BytesIO

os.makedirs("outputs/validation", exist_ok=True)

# 1. SAMPLE ICA files for column completeness

raw_zips = glob.glob("ica_data/raw_zips/*.zip")[:3]   # limit due to large size

rows_total = 0
rows_sg = 0
rows_of = 0
rows_L = 0
rows_G = 0

ica_keys = set()

for div in raw_zips:
    with zipfile.ZipFile(div, "r") as dz:
        inner = [n for n in dz.namelist() if n.lower().endswith(".zip")][:40]

        for name in inner:
            try:
                inner_bytes = dz.read(name)
            except:
                continue

            with zipfile.ZipFile(BytesIO(inner_bytes), "r") as iz:
                csvs = [c for c in iz.namelist() if c.endswith(".csv")]
                if not csvs:
                    continue
                
                df = pd.read_csv(iz.open(csvs[0]), low_memory=False)
                df.columns = df.columns.str.lower().str.strip()

            rows_total += len(df)
            rows_sg += df["hourly_ica_sg"].notna().sum() if "hourly_ica_sg" in df else 0
            rows_of += df["hourly_ica_of"].notna().sum() if "hourly_ica_of" in df else 0

            if "loadorgen" in df:
                load = df["loadorgen"].astype(str).str.upper()
                rows_L += (load == "L").sum()
                rows_G += (load == "G").sum()

            # month-hour keys for later coverage check
            if {"month", "hour"}.issubset(df.columns):
                for m, h in zip(df["month"], df["hour"]):
                    ica_keys.add((int(m), int(h)))

summary = pd.DataFrame({
    "total_rows": [rows_total],
    "non_na_hourly_ica_sg": [rows_sg],
    "non_na_hourly_ica_of": [rows_of],
    "load_rows_L": [rows_L],
    "gen_rows_G": [rows_G]
})

summary.to_csv("outputs/validation/ica_raw_summary.csv", index=False)


# 2. month hour ICA coverage

df_keys = pd.read_csv("outputs/feeder_load_features.csv", low_memory=False)
df_keys = df_keys[["month", "hour"]].drop_duplicates()
df_key_set = set((int(m), int(h)) for m, h in zip(df_keys["month"], df_keys["hour"]))

missing = sorted(list(df_key_set - ica_keys))

coverage_df = pd.DataFrame(missing, columns=["month", "hour"])
coverage_df.to_csv("outputs/validation/missing_ica_month_hour.csv", index=False)


# 3. CALMAC GP + load shape check

calmac = pd.read_csv("CALMAC/Res_GP_Elec_2024.csv", low_memory=False)
calmac["date"] = pd.to_datetime(calmac["date"])
calmac["month"] = calmac["date"].dt.month

calmac_summary = calmac.groupby(["gp", "month", "hour"]).agg({"kwh": "count"})
calmac_summary.to_csv("outputs/validation/calmac_month_hour_counts.csv")
