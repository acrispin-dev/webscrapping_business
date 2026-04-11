#!/usr/bin/env python3
"""
Consolidate CSV files with improved SKUs
Combines latest Bembos + Popeyes (with improved SKUs) + KFC (from previous run)
"""
import pandas as pd
from pathlib import Path

OUTPUT_DIR = Path("output")

# Read individual brand files
print("Reading individual brand files...")
df_bembos = pd.read_csv(OUTPUT_DIR / "precios_bembos.csv")
df_popeyes = pd.read_csv(OUTPUT_DIR / "precios_popeyes.csv")

# Read KFC from old consolidated (since new KFC run failed)
df_consolidated_old = pd.read_csv(OUTPUT_DIR / "consolidated_precios.csv")
df_kfc = df_consolidated_old[df_consolidated_old['sku_master'].str.contains('KFC_', na=False)].copy()

print(f"Bembos: {len(df_bembos)} rows")
print(f"Popeyes: {len(df_popeyes)} rows")
print(f"KFC: {len(df_kfc)} rows")

# Combine
df_consolidated = pd.concat([df_bembos, df_popeyes, df_kfc], ignore_index=True)
print(f"\nConsolidated: {len(df_consolidated)} rows")

# Save
df_consolidated.to_csv(OUTPUT_DIR / "consolidated_precios.csv", index=False, encoding="utf-8")
print(f"✓ Saved: consolidated_precios.csv ({len(df_consolidated)} rows)")

# Do the same for maestro files
print("\nConsolidating maestro files...")
df_maestro_bembos = pd.read_csv(OUTPUT_DIR / "maestro_bembos.csv")
df_maestro_popeyes = pd.read_csv(OUTPUT_DIR / "maestro_popeyes.csv")
df_maestro_kfc = df_consolidated_old[df_consolidated_old['sku_master'].str.contains('KFC_', na=False)].copy()
df_maestro_kfc = df_maestro_kfc[[col for col in df_maestro_kfc.columns if col not in ['precio_regular', 'fecha_inicio', 'fecha_fin', 'fuente_precio', 'url_fuente', 'precio_base_fuente']]].drop_duplicates(subset=['sku_master']).reset_index(drop=True)

df_maestro_consolidated = pd. concat([df_maestro_bembos, df_maestro_popeyes, df_maestro_kfc], ignore_index=True)
df_maestro_consolidated = df_maestro_consolidated.drop_duplicates(subset=['sku_master']).reset_index(drop=True)
print(f"Consolidated maestro: {len(df_maestro_consolidated)} rows")

df_maestro_consolidated.to_csv(OUTPUT_DIR / "consolidated_maestro.csv", index=False, encoding="utf-8")
print(f"✓ Saved: consolidated_maestro.csv ({len(df_maestro_consolidated)} rows)")

# Do the same for raw data
print("\nConsolidating raw data files...")
df_raw_bembos = pd.read_csv(OUTPUT_DIR / "datos_raw_bembos.csv")
df_raw_popeyes = pd.read_csv(OUTPUT_DIR / "datos_raw_popeyes.csv")
df_raw_kfc_consolidated = df_consolidated_old[df_consolidated_old['sku_master'].str.contains('KFC_', na=False)].copy()
# Extract raw KFC from old raw file if exists
try:
    df_raw_kfc = pd.read_csv(OUTPUT_DIR / "datos_raw_kfc.csv")
except:
    df_raw_kfc = df_raw_kfc_consolidated.copy()

df_raw_consolidated = pd.concat([df_raw_bembos, df_raw_popeyes, df_raw_kfc], ignore_index=True)
print(f"Consolidated raw: {len(df_raw_consolidated)} rows")

df_raw_consolidated.to_csv(OUTPUT_DIR / "consolidated_raw.csv", index=False, encoding="utf-8")
print(f"✓ Saved: consolidated_raw.csv ({len(df_raw_consolidated)} rows)")

print("\n✓ Consolidation complete!")
print(f"  - Precios: {len(df_consolidated)} rows")
print(f"  - Maestro: {len(df_maestro_consolidated)} rows")
print(f"  - Raw: {len(df_raw_consolidated)} rows")
