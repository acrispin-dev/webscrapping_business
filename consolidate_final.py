"""
Consolidate updated Bembos, Popeyes, and KFC data
"""
import pandas as pd
from pathlib import Path

output_dir = Path('output')

# Read individual brand precios files
precios_bembos = pd.read_csv(output_dir / 'precios_bembos.csv')
precios_popeyes = pd.read_csv(output_dir / 'precios_popeyes.csv')
precios_kfc = pd.read_csv(output_dir / 'precios_kfc.csv')

# Read individual brand maestro files
maestro_bembos = pd.read_csv(output_dir / 'maestro_bembos.csv')
maestro_popeyes = pd.read_csv(output_dir / 'maestro_popeyes.csv')
maestro_kfc = pd.read_csv(output_dir / 'maestro_kfc.csv')

# Read individual brand raw data files
raw_bembos = pd.read_csv(output_dir / 'datos_raw_bembos.csv')
raw_popeyes = pd.read_csv(output_dir / 'datos_raw_popeyes.csv')
raw_kfc = pd.read_csv(output_dir / 'datos_raw_kfc.csv')

# Consolidate precios
consolidated_precios = pd.concat([precios_bembos, precios_popeyes, precios_kfc], ignore_index=True)
consolidated_precios.to_csv(output_dir / 'consolidated_precios.csv', index=False)
print(f"✓ Consolidated precios: {len(consolidated_precios)} rows")

# Consolidate maestro (deduplicate)
consolidated_maestro = pd.concat([maestro_bembos, maestro_popeyes, maestro_kfc], ignore_index=True)
consolidated_maestro = consolidated_maestro.drop_duplicates(subset=['sku_master']).reset_index(drop=True)
consolidated_maestro.to_csv(output_dir / 'consolidated_maestro.csv', index=False)
print(f"✓ Consolidated maestro: {len(consolidated_maestro)} rows (deduplicated)")

# Consolidate raw data
consolidated_raw = pd.concat([raw_bembos, raw_popeyes, raw_kfc], ignore_index=True)
consolidated_raw.to_csv(output_dir / 'consolidated_raw.csv', index=False)
print(f"✓ Consolidated raw data: {len(consolidated_raw)} rows")

print("\n✓ All files consolidated successfully!")
print(f"  - consolidated_precios.csv: {len(consolidated_precios)} rows")
print(f"  - consolidated_maestro.csv: {len(consolidated_maestro)} unique SKUs")
print(f"  - consolidated_raw.csv: {len(consolidated_raw)} rows")

# Show sample of consolidated data with improved SKUs
print("\nSample of consolidated precios with improved SKUs:")
print(consolidated_precios[['sku_master', 'sku_comparable', 'precio_regular']].sample(min(10, len(consolidated_precios))))
