import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="whitegrid")

# file paths
PATHS = {
    "res_characteristics": "CALMAC/res_characteristics.csv",
    "res_centroids": "CALMAC/res_centroids.csv",
    "res_gp_elec_2024": "CALMAC/Res_GP_Elec_2024.csv",

    "nonres_characteristics": "CALMAC/nonres_characteristics.csv",
    "nonres_centroids": "CALMAC/nonres_centroids.csv",
    "nonres_segments_counts": "CALMAC/Nonres_Elec_GP_Segments_and_Counts.csv",
}

# load data

dfs = {}
for name, path in PATHS.items():
    df_tmp = pd.read_csv(path)
    dfs[name] = df_tmp
    print(f"{name}: loaded from {path} — shape = {df_tmp.shape}")

# prepare centroid + region data

# Residential centroids + climate zone (seg_cz)
res_char = dfs["res_characteristics"]
res_cent = dfs["res_centroids"]
res_char_region = res_char[["gp", "seg_cz"]]
res_cent_merged = res_cent.merge(res_char_region, on="gp", how="left")

print("\nResidential centroids with region (seg_cz):")
print(res_cent_merged.head())

# nonresidential centroids + climate zone (seg_cz)
nonres_char = dfs["nonres_characteristics"]
nonres_cent = dfs["nonres_centroids"]
nonres_char_region = nonres_char[["gp", "seg_cz"]].drop_duplicates()
nonres_cent_merged = nonres_cent.merge(nonres_char_region, on="gp", how="left")

print("\nNonresidential centroids with region (seg_cz):")
print(nonres_cent_merged.head())

# plot residential centroid map by region
plt.figure(figsize=(7, 6))
sns.scatterplot(
    data=res_cent_merged,
    x="longitude",
    y="latitude",
    hue="seg_cz",      # region
    palette="Set2",
    s=40,
    alpha=0.9,
)
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title("Residential GP Centroids by Region")
plt.legend(title="Region", bbox_to_anchor=(1.05, 1), loc="upper left")

plt.tight_layout()
plt.show()

# plot nonresidential centroid map by region
plt.figure(figsize=(7, 6))
sns.scatterplot(
    data=nonres_cent_merged,
    x="longitude",
    y="latitude",
    hue="seg_cz",      # region
    palette="Set2",
    s=40,
    alpha=0.9,
)
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title("Nonresidential GP Centroids by Region")
plt.legend(title="Region", bbox_to_anchor=(1.05, 1), loc="upper left")

plt.tight_layout()
plt.show()

# residential average hourly kWh profile
res_gp = dfs["res_gp_elec_2024"]

# average kWh by hour across all segments and days
avg_hourly = (
    res_gp.groupby("hour")["kwh"]
    .mean()
    .reset_index()
    .sort_values("hour")
)

print("\nAverage residential kWh by hour:")
print(avg_hourly.head())

plt.figure(figsize=(8, 4))
sns.lineplot(data=avg_hourly, x="hour", y="kwh")
plt.xlabel("Hour of Day")
plt.ylabel("Average kWh per Premise")
plt.title("Average Residential Hourly Load Shape (All GPs, 2024)")
plt.xticks(range(0, 24, 2))
plt.tight_layout()

plt.show()

# top 10 nonres segments by premise count
nonres_seg = dfs["nonres_segments_counts"]

# sum prem_count by segment industry (seg_ind)
seg_totals = (
    nonres_seg.groupby("seg_ind")["prem_count"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

print("\nTop 10 nonres segments by prem_count:")
print(seg_totals)

plt.figure(figsize=(9, 4))
seg_totals.sort_values().plot(kind="barh")  # horizontal for readability
plt.xlabel("Total Premise Count")
plt.ylabel("Nonres Segment (seg_ind)")
plt.title("Top 10 Nonresidential Segments by Premise Count")
plt.tight_layout()

plt.show()

print("\nAll plots have been displayed.")
