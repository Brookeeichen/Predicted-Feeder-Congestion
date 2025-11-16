# Predicted Feeder Congestion - Data Wrangling

This project processes climate zones, zip codes, and CALMAC load shape data to create a merged dataset for feeder congestion analysis.

## Files Created from Notebook

- `data_wrangling.py` - Main data processing script extracted from Data Wrangling.ipynb
- `requirements.txt` - Python dependencies needed to run the script
- `README.md` - This file

## Data Structure

### Input Data
- `CALMAC/Building_Climate_Zones.shp` - CEC climate zone shapefile
- `CALMAC/res_characteristics.csv` - Residential characteristics
- `CALMAC/nonres_characteristics.csv` - Non-residential characteristics  
- `zip_codes/zip_poly.shp` - ZIP code polygon shapefile

### Output Data
- `zip_codes/zips_final.csv` - Merged dataset with aggregated load shapes
- `zip_codes/zips_expanded.csv` - Expanded dataset (one row per load shape)

## How to Run

### Option 1: Install Python locally
```bash
# Install dependencies
pip install -r requirements.txt

# Run the data wrangling script
python data_wrangling.py
```

### Option 2: Use online Python environments
- Upload the project folder to Google Colab
- Use JupyterLite in browser
- Use GitHub Codespaces

### Option 3: VS Code
- Open the project in VS Code
- Install Python extension
- Run the script in the integrated terminal

## Data Processing Steps

1. **Load Climate Zones**: Load CEC climate zone shapefile and map to CALMAC territories
2. **Process ZIP Codes**: Load ZIP code polygons and find centroids
3. **Spatial Join**: Map ZIP codes to climate zones using spatial join
4. **Load CALMAC Data**: Combine residential and non-residential characteristics
5. **Group by Climate Zone**: Group load profiles by climate zone
6. **Merge Datasets**: Combine ZIP codes with load shape data
7. **Expand Load Shapes**: Create one row per load shape for analysis

## Climate Zone Mapping

The script maps CEC climate zones to CALMAC territories:
- **Coastal**: Zones 1, 3, 5
- **Inland**: Zones 2, 4  
- **North Central Valley**: Zones 11, 12
- **South Central Valley**: Zone 13

## Output Schema

### Final Dataset (zips_final.csv)
- ZIP_CODE, POPULATION, POP_SQMI, SQMI
- geometry, centroid (geographic data)
- BZone, cz_groups, seg_cz (climate zone info)
- gp_list (list of load shapes for each ZIP)

### Expanded Dataset (zips_expanded.csv)
Same as above but with one row per load shape:
- gp (individual load shape identifier)

## Load Shape Naming Convention

Load shapes follow the pattern: `{season}_{type}_{sector}_{zone}`
- Season: 1, 2, 3, 4
- Type: NS (non-summer)
- Sector: C (Coastal), I (Inland), N (North Central), S (South Central)

Example: `1_1_NS_I` = Season 1, Type 1, Non-summer, Inland zone
