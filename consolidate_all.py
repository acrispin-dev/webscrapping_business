#!/usr/bin/env python
"""Consolidate Pizza Hut prices from individual files"""

import pandas as pd
from pathlib import Path

# Load all prix files
output_dir = Path("output")

precios_files = {
    "bembos": output_dir / "precios_bembos.csv",
    "kfc": output_dir / "precios_kfc.csv",
    "pizzahut": output_dir / "precios_pizzahut.csv",
    "popeyes": output_dir / "precios_popeyes.csv",
}

dfs = []
for name, file_path in precios_files.items():
    if file_path.exists():
        df = pd.read_csv(file_path)
        dfs.append(df)
        print(f"Loaded {name}: {len(df)} rows")
    else:
        print(f"NOT FOUND: {file_path}")

if dfs:
    consolidated = pd.concat(dfs, ignore_index=True).drop_duplicates()
    consolidated.to_csv(output_dir / "consolidated_precios.csv", index=False)
    print(f"\n✅ Saved consolidated_precios.csv: {len(consolidated)} rows")
    
    # Display Pizza Hut items as verification
    pizzahut_data = consolidated[consolidated['sku_master'].str.contains('PIZZAHUT', na=False)]
    print(f"\n🍕 PIZZA HUT ENTRIES: {len(pizzahut_data)}")
    
    # Group and show examples
    for familia in ['Antojito', 'Bebida']:
        subset = pizzahut_data[pizzahut_data['sku_master'].str.contains('_SALSA|AGUA|COCA|SPRITE|FANTA|INCA', regex=True, na=False)]
        if not subset.empty:
            print(f"\n{familia} Examples:")
            for _, row in subset.head(5).iterrows():
                sku = row['sku_master']
                precio = row['precio_regular']
                print(f"  {sku:45} | S/ {precio}")
else:
    print("No files to consolidate!")
