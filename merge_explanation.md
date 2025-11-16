# Exploring Load Shape Data Merge with zips_final

## Data Structure Overview

### 1. Load Shape Data (Res_GP_Elec_2024.csv)
```
gp,           date,       hour, kwh
"1_1_NS_C",   2024-04-01, 0,    0.1598675
"1_1_NS_C",   2024-04-01, 1,    0.1490723
"1_1_NS_C",   2024-04-01, 2,    0.143743
...
```
- **Format**: Long format (one row per hour per load profile)
- **Size**: ~50MB, millions of records
- **Time period**: 2024 data (April through at least some period)
- **Load profiles**: Like "1_1_NS_C", "2_1_NS_C", etc.

### 2. Characteristics Data (res_characteristics.csv)
```
gp,       seg_cz,  seg_solar, seg_size, seg_shape, prem_count
"1_1_NS_C","Coastal","NS",    1,        1,        250
"2_1_NS_C","Coastal","NS",    1,        2,        250
"3_1_NS_C","Coastal","NS",    1,        3,        250
...
```
- **Format**: One row per load profile
- **Purpose**: Maps load profiles to climate zones and characteristics
- **Climate zones**: Coastal, Inland, North Central Valley, South Central Valley

### 3. zips_final Structure (after spatial processing)
```
ZIP_CODE, POPULATION, cz_groups,           gp_list
90210,    10000,      "Coastal",          ["1_1_NS_C", "2_1_NS_C"]
90211,    15000,      "Coastal",          ["1_1_NS_C", "3_1_NS_C"]  
90212,    12000,      "Inland",           ["2_1_NS_I", "4_1_NS_I"]
...
```
- **Format**: One row per ZIP code
- **gp_list**: List of load profiles applicable to that ZIP's climate zone

## Merge Process Steps

### Step 1: Expand zips_final
**From:**
```
ZIP_CODE: 90210, gp_list: ["1_1_NS_C", "2_1_NS_C"]
```

**To (after explode):**
```
ZIP_CODE: 90210, gp: "1_1_NS_C"
ZIP_CODE: 90210, gp: "2_1_NS_C"
```

### Step 2: Merge with Load Data
**Join on: gp column**

**Result:**
```
ZIP_CODE, gp,        date,       hour, kwh
90210,    "1_1_NS_C",2024-04-01, 0,    0.1598675
90210,    "1_1_NS_C",2024-04-01, 1,    0.1490723
90210,    "1_1_NS_C",2024-04-01, 2,    0.143743
90210,    "2_1_NS_C",2024-04-01, 0,    [load data]
90210,    "2_1_NS_C",2024-04-01, 1,    [load data]
...
```

## Key Insights

### Load Profile Naming Convention
- **Format**: `{size}_{season}_{solar}_{zone}`
- **Examples**:
  - `1_1_NS_C` = Size 1, Season 1, Non-Summer, Coastal
  - `2_1_NS_I` = Size 2, Season 1, Non-Summer, Inland

### Data Volume Impact
- **Before merge**: ~1,000 ZIP codes × ~5 load profiles each = ~5,000 rows
- **After merge**: ~5,000 load profiles × 24 hours × 365 days = ~43,800,000 rows
- **Size increase**: From KB to potentially GB scale

### Merge Strategy Options

#### Option 1: Full Merge (Current Implementation)
- **Pros**: Complete hourly data for every ZIP
- **Cons**: Very large dataset, memory intensive
- **Use case**: Detailed hourly analysis

#### Option 2: Aggregated Merge
- **Approach**: Aggregate load data to daily/monthly before merging
- **Pros**: Smaller dataset, faster processing
- **Cons**: Loses hourly granularity
- **Use case**: Daily/monthly analysis

#### Option 3: Sample Merge  
- **Approach**: Sample of ZIP codes or time period
- **Pros**: Manageable size for testing
- **Cons**: Not complete dataset
- **Use case**: Development and testing

## Potential Issues to Watch

### 1. Memory Usage
The full merge could create 40M+ rows, which may exceed available RAM.

### 2. Processing Time
Merging and processing 40M rows can take significant time.

### 3. File Size
Output CSV could be several GB, making it hard to open in Excel.

### 4. Missing Data
Some load profiles in characteristics might not exist in load data.

## Recommendations

### For Development:
1. Start with a sample (e.g., 10 ZIP codes, 1 week of data)
2. Test the merge process with smaller data
3. Verify data integrity and relationships

### For Production:
1. Consider using Parquet format instead of CSV for better compression
2. Implement chunked processing for large datasets
3. Add data validation checks
4. Consider database storage for very large datasets

## Next Steps

Would you like me to:
1. Create a sample merge with limited data?
2. Implement aggregated merging (daily/monthly)?
3. Add memory-efficient processing?
4. Create data validation checks?
