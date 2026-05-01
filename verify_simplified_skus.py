#!/usr/bin/env python
"""Test Pizza Hut scraper with simplified SKUs"""

from scrapers.pizzahut_scraper import PizzaHutScraper

scraper = PizzaHutScraper()
df = scraper.execute(save_local=True)

print("\n" + "="*100)
print("PIZZA HUT RESULTS - CHECKING SIMPLIFIED SKU_MASTER")
print("="*100)

# Show antojitos with their new SKU_MASTER
antojitos = df[df['familia_producto'] == 'Antojito']
print(f"\n✓ ANTOJITOS ({len(antojitos)} rows):")
print("-"*100)
for _, row in antojitos.iterrows():
    print(f"SKU: {row['sku_master']:45} | Item: {row['item_canonico'][:40]}")

# Show bebidas with their new SKU_MASTER
bebidas = df[df['familia_producto'] == 'Bebida']
print(f"\n✓ BEBIDAS ({len(bebidas)} rows):")
print("-"*100)
for _, row in bebidas.iterrows():
    print(f"SKU: {row['sku_master']:45} | Item: {row['item_canonico'][:40]}")

print("\n✅ Scraper finished successfully!")
