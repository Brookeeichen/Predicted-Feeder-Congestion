"""
Examine zips_with_loads Structure
This script shows what the final merged dataset will look like
"""

import pandas as pd
import numpy as np

def create_realistic_zips_with_loads_sample():
    """Create a realistic sample of what zips_with_loads will contain"""
    
    print("=== CREATING REALISTIC zips_with_loads SAMPLE ===\n")
    
    # Sample data based on actual structure
    data = []
    
    # ZIP codes with different characteristics
    zip_data = [
        {'ZIP_CODE': 90210, 'POPULATION': 10234, 'cz_groups': 'Coastal', 'geometry': 'POINT(-118.4 34.07)'},
        {'ZIP_CODE': 90211, 'POPULATION': 8567, 'cz_groups': 'Coastal', 'geometry': 'POINT(-118.41 34.08)'},
        {'ZIP_CODE': 94102, 'POPULATION': 15432, 'cz_groups': 'Coastal', 'geometry': 'POINT(-122.42 37.78)'},
        {'ZIP_CODE': 95603, 'POPULATION': 7890, 'cz_groups': 'Inland', 'geometry': 'POINT(-121.5 38.77)'},
        {'ZIP_CODE': 95814, 'POPULATION': 12345, 'cz_groups': 'Inland', 'geometry': 'POINT(-121.5 38.58)'},
    ]
    
    # Load profiles for each climate zone
    load_profiles = {
        'Coastal': ['1_1_NS_C', '2_1_NS_C', '3_1_NS_C', '4_1_NS_C'],
        'Inland': ['1_1_NS_I', '2_1_NS_I', '3_1_NS_I', '4_1_NS_I']
    }
    
    # Sample dates (first week of April)
    dates = ['2024-04-01', '2024-04-02', '2024-04-03']
    
    # Create records
    record_id = 0
    for zip_info in zip_data:
        zip_code = zip_info['ZIP_CODE']
        population = zip_info['POPULATION']
        climate_zone = zip_info['cz_groups']
        geometry = zip_info['geometry']
        
        # Get applicable load profiles
        profiles = load_profiles[climate_zone]
        
        for gp in profiles:
            for date in dates:
                for hour in range(24):  # All 24 hours
                    # Generate realistic kWh values based on hour and profile
                    base_kwh = 0.15 if climate_zone == 'Coastal' else 0.18
                    
                    # Daily pattern (higher during day, lower at night)
                    if 6 <= hour <= 9:  # Morning peak
                        hour_multiplier = 1.3
                    elif 17 <= hour <= 21:  # Evening peak
                        hour_multiplier = 1.5
                    elif 0 <= hour <= 5:  # Night
                        hour_multiplier = 0.7
                    else:  # Day
                        hour_multiplier = 1.0
                    
                    # Profile variation
                    profile_multiplier = int(gp.split('_')[0]) / 2.5
                    
                    kwh = base_kwh * hour_multiplier * profile_multiplier * np.random.normal(1.0, 0.1)
                    
                    data.append({
                        'ZIP_CODE': zip_code,
                        'POPULATION': population,
                        'cz_groups': climate_zone,
                        'geometry': geometry,
                        'centroid': geometry,
                        'BZone': 1 if climate_zone == 'Coastal' else 2,
                        'seg_cz': climate_zone,
                        'gp': gp,
                        'date': date,
                        'hour': hour,
                        'kwh': max(0, kwh),  # Ensure non-negative
                        'record_id': record_id
                    })
                    record_id += 1
    
    return pd.DataFrame(data)

def examine_structure(df):
    """Examine the structure and properties of zips_with_loads"""
    
    print("1. DATASET OVERVIEW:")
    print(f"   Total records: {len(df):,}")
    print(f"   ZIP codes: {df['ZIP_CODE'].nunique()}")
    print(f"   Climate zones: {df['cz_groups'].nunique()}")
    print(f"   Load profiles: {df['gp'].nunique()}")
    print(f"   Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"   Hours per day: {df.groupby(['ZIP_CODE', 'gp', 'date']).size().iloc[0]}")
    
    print(f"\n2. COLUMN DETAILS:")
    for col in df.columns:
        dtype = df[col].dtype
        unique_count = df[col].nunique()
        null_count = df[col].isnull().sum()
        
        if col in ['ZIP_CODE', 'POPULATION', 'hour', 'kwh']:
            sample_val = f"min={df[col].min()}, max={df[col].max()}, avg={df[col].mean():.2f}"
        else:
            sample_vals = df[col].dropna().head(2).tolist()
            sample_val = str(sample_vals)
        
        print(f"   {col:15} | {dtype:10} | {unique_count:4} unique | {null_count:3} null | {sample_val}")
    
    print(f"\n3. SAMPLE RECORDS (first 10):")
    print(df.head(10).to_string(index=False))
    
    print(f"\n4. DATA DISTRIBUTION:")
    
    # By ZIP code
    print(f"\n   Records by ZIP code:")
    zip_counts = df.groupby('ZIP_CODE').size()
    for zip_code, count in zip_counts.items():
        print(f"     ZIP {zip_code}: {count:,} records")
    
    # By climate zone
    print(f"\n   Records by climate zone:")
    zone_counts = df.groupby('cz_groups').size()
    for zone, count in zone_counts.items():
        print(f"     {zone}: {count:,} records")
    
    # Load profile analysis
    print(f"\n5. LOAD PROFILE ANALYSIS:")
    profile_stats = df.groupby('gp')['kwh'].agg(['count', 'mean', 'min', 'max', 'std']).round(4)
    print(profile_stats)
    
    # Hourly patterns
    print(f"\n6. HOURLY CONSUMPTION PATTERNS:")
    hourly_avg = df.groupby('hour')['kwh'].mean().round(4)
    print("   Hour | Avg kWh")
    for hour, avg_kwh in hourly_avg.items():
        bar = '█' * int(avg_kwh * 20)  # Simple bar chart
        print(f"   {hour:02d}   | {avg_kwh:.4f} {bar}")
    
    return df

def show_analysis_potential(df):
    """Show what analyses are possible with this dataset"""
    
    print(f"\n7. ANALYSIS CAPABILITIES:")
    
    print(f"\n   ✓ Time Series Analysis:")
    print(f"     - Daily/weekly/monthly consumption patterns")
    print(f"     - Peak demand identification")
    print(f"     - Load factor calculations")
    
    print(f"\n   ✓ Geographic Analysis:")
    print(f"     - Climate zone comparisons")
    print(f"     - ZIP code ranking by consumption")
    print(f"     - Regional load distribution")
    
    print(f"\n   ✓ Load Profile Analysis:")
    print(f"     - Profile performance comparison")
    print(f"     - Profile optimization opportunities")
    print(f"     - Seasonal variations")
    
    print(f"\n   ✓ Feeder Congestion Analysis:")
    print(f"     - Peak load forecasting")
    print(f"     - Capacity utilization")
    print(f"     - Load growth projections")
    
    # Calculate some example metrics
    print(f"\n8. KEY METRICS FROM SAMPLE DATA:")
    
    total_consumption = df['kwh'].sum()
    avg_per_zip = df.groupby('ZIP_CODE')['kwh'].sum().mean()
    peak_hour = df.groupby('hour')['kwh'].sum().idxmax()
    peak_consumption = df.groupby('hour')['kwh'].sum().max()
    
    print(f"   Total consumption (sample): {total_consumption:,.2f} kWh")
    print(f"   Average per ZIP code: {avg_per_zip:,.2f} kWh")
    print(f"   Peak hour: {peak_hour:02d}:00 with {peak_consumption:,.2f} kWh")
    print(f"   Coastal vs Inland ratio: {df[df['cz_groups']=='Coastal']['kwh'].sum() / df[df['cz_groups']=='Inland']['kwh'].sum():.2f}")
    
    return df

def estimate_full_dataset_size(df):
    """Estimate the size of the full dataset"""
    
    print(f"\n9. FULL DATASET SIZE ESTIMATION:")
    
    sample_zips = df['ZIP_CODE'].nunique()
    sample_days = (pd.to_datetime(df['date'].max()) - pd.to_datetime(df['date'].min())).days + 1
    sample_records = len(df)
    
    # Estimate for full dataset
    estimated_zips = 1000  # Approximate number of ZIP codes in CA
    full_year_days = 365
    
    estimated_records = (sample_records / sample_zips / sample_days) * estimated_zips * full_year_days
    
    print(f"   Current sample: {sample_records:,} records")
    print(f"   Sample covers: {sample_zips} ZIP codes, {sample_days} days")
    print(f"   Full year estimate: {estimated_records:,:,} records")
    print(f"   Expansion factor: {estimated_records/sample_records:.0f}x")
    
    # File size estimation
    avg_row_size = 100  # bytes per row (rough estimate)
    estimated_size_mb = (estimated_records * avg_row_size) / (1024 * 1024)
    estimated_size_gb = estimated_size_mb / 1024
    
    print(f"   Estimated file size: {estimated_size_mb:.0f} MB ({estimated_size_gb:.1f} GB)")
    
    return estimated_records

def main():
    """Main examination function"""
    
    print("Examination of zips_with_loads Structure")
    print("=" * 50)
    
    # Create realistic sample
    df = create_realistic_zips_with_loads_sample()
    
    # Examine structure
    examine_structure(df)
    
    # Show analysis potential
    show_analysis_potential(df)
    
    # Estimate full size
    estimate_full_dataset_size(df)
    
    print(f"\n=== EXAMINATION COMPLETE ===")
    print("This shows exactly what zips_with_loads will contain!")
    
    return df

if __name__ == "__main__":
    result = main()
