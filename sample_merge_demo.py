"""
Sample Merge Demonstration
Shows how zips_expanded merges with load data step by step
"""

import pandas as pd
import sys

def create_sample_data():
    """Create sample datasets to demonstrate the merge process"""
    
    print("=== CREATING SAMPLE DATA ===\n")
    
    # Sample zips_expanded (normally from spatial processing + explode)
    sample_zips_expanded = pd.DataFrame({
        'ZIP_CODE': [90210, 90210, 90211, 90211, 90212, 90212],
        'POPULATION': [10000, 10000, 15000, 15000, 12000, 12000],
        'cz_groups': ['Coastal', 'Coastal', 'Coastal', 'Coastal', 'Inland', 'Inland'],
        'gp': ['1_1_NS_C', '2_1_NS_C', '1_1_NS_C', '3_1_NS_C', '2_1_NS_I', '4_1_NS_I']
    })
    
    print("1. Sample zips_expanded:")
    print(sample_zips_expanded)
    print(f"\n   Shape: {sample_zips_expanded.shape}")
    print(f"   ZIP codes: {sample_zips_expanded['ZIP_CODE'].nunique()}")
    print(f"   Load profiles: {sample_zips_expanded['gp'].nunique()}")
    
    # Sample load data (subset of actual load data)
    sample_load_data = pd.DataFrame({
        'gp': ['1_1_NS_C', '1_1_NS_C', '1_1_NS_C', '1_1_NS_C', 
               '2_1_NS_C', '2_1_NS_C', '2_1_NS_C', '2_1_NS_C',
               '3_1_NS_C', '3_1_NS_C', '3_1_NS_C', '3_1_NS_C',
               '2_1_NS_I', '2_1_NS_I', '2_1_NS_I', '2_1_NS_I'],
        'date': ['2024-04-01', '2024-04-01', '2024-04-01', '2024-04-01',
                 '2024-04-01', '2024-04-01', '2024-04-01', '2024-04-01',
                 '2024-04-01', '2024-04-01', '2024-04-01', '2024-04-01',
                 '2024-04-01', '2024-04-01', '2024-04-01', '2024-04-01'],
        'hour': [0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3],
        'kwh': [0.1598675, 0.1490723, 0.143743, 0.1406948,
                0.165234, 0.154123, 0.148562, 0.145678,
                0.172456, 0.161234, 0.155891, 0.152345,
                0.182345, 0.171234, 0.165678, 0.162123]
    })
    
    print("\n2. Sample load data:")
    print(sample_load_data)
    print(f"\n   Shape: {sample_load_data.shape}")
    print(f"   Load profiles: {sample_load_data['gp'].nunique()}")
    print(f"   Date range: {sample_load_data['date'].min()} to {sample_load_data['date'].max()}")
    print(f"   Hours per profile: {sample_load_data.groupby('gp').size().iloc[0]}")
    
    return sample_zips_expanded, sample_load_data

def demonstrate_merge(zips_expanded, load_data):
    """Demonstrate the merge process step by step"""
    
    print("\n=== MERGE PROCESS ===\n")
    
    # Step 1: Check data compatibility
    print("3. Checking data compatibility...")
    zip_gps = set(zips_expanded['gp'].unique())
    load_gps = set(load_data['gp'].unique())
    
    print(f"   Load profiles in zips_expanded: {zip_gps}")
    print(f"   Load profiles in load_data: {load_gps}")
    print(f"   Overlapping profiles: {zip_gps.intersection(load_gps)}")
    print(f"   Missing from load data: {zip_gps - load_gps}")
    
    # Step 2: Perform the merge
    print("\n4. Performing merge...")
    merged_data = zips_expanded.merge(load_data, on='gp', how='left')
    
    print(f"   Merge completed!")
    print(f"   Original zips rows: {len(zips_expanded)}")
    print(f"   Load data rows: {len(load_data)}")
    print(f"   Merged rows: {len(merged_data)}")
    
    # Step 3: Show merged data
    print("\n5. Merged dataset:")
    print(merged_data)
    
    # Step 4: Analyze results
    print("\n6. Merge analysis:")
    print(f"   Records with load data: {merged_data['kwh'].notna().sum()}/{len(merged_data)}")
    print(f"   Missing load data: {merged_data['kwh'].isna().sum()}")
    
    if merged_data['kwh'].isna().any():
        missing_gps = merged_data[merged_data['kwh'].isna()]['gp'].unique()
        print(f"   Missing load profiles: {missing_gps}")
    
    # Step 5: Show data by ZIP code
    print("\n7. Data by ZIP code:")
    zip_summary = merged_data.groupby('ZIP_CODE').agg({
        'gp': 'nunique',
        'date': 'nunique', 
        'hour': 'count',
        'kwh': 'sum'
    }).round(2)
    zip_summary.columns = ['Load_Profiles', 'Dates', 'Hourly_Records', '_Total_kWh']
    print(zip_summary)
    
    return merged_data

def show_data_structure(merged_data):
    """Show the structure and potential of the merged data"""
    
    print("\n=== DATA STRUCTURE ANALYSIS ===\n")
    
    print("8. Column analysis:")
    for col in merged_data.columns:
        dtype = merged_data[col].dtype
        unique_count = merged_data[col].nunique()
        sample_values = merged_data[col].dropna().head(3).tolist()
        print(f"   {col}: {dtype} ({unique_count} unique) - Sample: {sample_values}")
    
    print("\n9. Potential analysis capabilities:")
    print("   ✓ Hourly load analysis by ZIP code")
    print("   ✓ Climate zone comparison")
    print("   ✓ Load profile aggregation")
    print("   ✓ Time series analysis")
    print("   ✓ Geographic load distribution")
    
    print("\n10. Data volume estimation:")
    zip_count = merged_data['ZIP_CODE'].nunique()
    gp_count = merged_data['gp'].nunique()
    hours_per_day = 24
    days_in_year = 365
    
    estimated_rows = zip_count * gp_count * hours_per_day * days_in_year
    print(f"   Current sample: {len(merged_data):,} rows")
    print(f"   Full year estimate: {estimated_rows:,} rows")
    print(f"   Expansion factor: {estimated_rows/len(merged_data):.0f}x")
    
    return merged_data

def main():
    """Main demonstration function"""
    try:
        print("Sample Merge Demonstration for Load Shape Data")
        print("=" * 50)
        
        # Create sample data
        zips_expanded, load_data = create_sample_data()
        
        # Demonstrate merge
        merged_data = demonstrate_merge(zips_expanded, load_data)
        
        # Show data structure
        show_data_structure(merged_data)
        
        print("\n=== DEMONSTRATION COMPLETE ===")
        print("This shows how zips_expanded merges with load data!")
        print("The actual merge will follow the same pattern with much more data.")
        
        return merged_data
        
    except Exception as e:
        print(f"Error during demonstration: {e}")
        return None

if __name__ == "__main__":
    result = main()
