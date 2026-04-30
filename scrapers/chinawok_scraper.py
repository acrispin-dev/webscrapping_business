"""
Chinawok scraper - Web scraping for Chinawok menu items
Uses Playwright for dynamic content loading
"""

import asyncio
import re
import unicodedata
import pandas as pd
from playwright.async_api import async_playwright
from .base_scraper import BaseScraper
from typing import List, Dict, Optional


class ChinawokScraper(BaseScraper):
    """Scraper for Chinawok Chinese restaurant website"""
    
    # Base URL and menu categories
    BASE_URL = "https://www.chinawok.com.pe"
    CATEGORIES = {
        "clasicos": "clasicos",
        "sabor-al-wok": "sabor-al-wok",
        "a-lo-pobre": "a-lo-pobre",
        "mostrazo-chinawok": "mostrazo-chinawok",
        "familiar": "familiar",
        "complementos": "complementos",
    }
    
    def __init__(self, output_dir: str = "output"):
        """Initialize Chinawok scraper"""
        super().__init__(output_dir, marca="CHINAWOK")
        self.logger.info(f"Initialized ChinawokScraper")
    
    def scrape(self) -> pd.DataFrame:
        """
        Main scraping method using Playwright
        Scrapes all 6 Chinawok menu categories
        """
        self.logger.info("Starting Chinawok scraping...")
        
        # Run async scraping, handling cases where an event loop may already be running
        try:
            raw_data = asyncio.run(self._scrape_all_categories())
        except RuntimeError as e:
            # Fallback for environments where an event loop is already running
            if "cannot be called" not in str(e) and "already running" not in str(e):
                raise
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                raw_data = loop.run_until_complete(self._scrape_all_categories())
            finally:
                loop.close()
        
        # Convert to DataFrame
        df = pd.DataFrame(raw_data)
        
        if len(df) == 0:
            self.logger.warning("No products found")
            return df
        
        self.logger.info(f"Scraped {len(df)} products before consolidation")
        
        # Consolidate products with multiple quantities
        df = self._consolidate_quantity_variants(df)
        
        self.logger.info(f"Scraped {len(df)} products after consolidation")
        
        # Add missing required fields
        df['sku_comparable'] = df['sku_master']  # BaseScraper will generate comparable SKU
        df['precio_base_fuente'] = df['precio_regular']
        df['fecha_inicio'] = pd.Timestamp.today().normalize()
        df['fecha_fin'] = pd.NaT
        df['fuente_precio'] = f"web_{self.marca.lower()}"
        
        self.df_raw = df
        return df
    
    def _consolidate_quantity_variants(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Consolidate products with multiple quantity variants into single rows with calculated unit prices.
        
        Examples:
        - 3 WANTANES (5.9) + 4 WANTANES (7.9) + 6 WANTANES (9.9) + 12 WANTANES (17.9) 
          → WANTANES with calculated average unit price
        """
        
        consolidated_data = {}
        processed_indices = set()
        
        for idx, row in df.iterrows():
            if idx in processed_indices:
                continue
            
            product_name = row['nombre_producto'].strip()
            
            # Pattern 1: Number at start (e.g., "3 Wantanes", "6 Alitas al Wok")
            match_start = re.match(r'^(\d+)\s+(.+)$', product_name, re.IGNORECASE)
            # Pattern 2: Number at end (e.g., "Tequeños de Lomo Oriental x 3", "Tequeños de Lomo Oriental 3")
            match_end = re.match(r'^(.+?)\s+[xX]?\s*(\d+)$', product_name, re.IGNORECASE)
            
            if match_start:
                quantity = int(match_start.group(1))
                product_base = match_start.group(2).strip()
                
            elif match_end and not match_start:
                product_base = match_end.group(1).strip()
                quantity = int(match_end.group(2))
                
            else:
                # No quantity pattern, keep as is
                consolidated_data[row['sku_master']] = row
                processed_indices.add(idx)
                continue
            
            # Find all variants of this product
            variants = []
            unit_prices = []
            
            for idx2, row2 in df.iterrows():
                if idx2 in processed_indices:
                    continue
                
                name2 = row2['nombre_producto'].strip()
                
                # Try both patterns for the second row
                match2_start = re.match(r'^(\d+)\s+(.+)$', name2, re.IGNORECASE)
                match2_end = re.match(r'^(.+?)\s+[xX]?\s*(\d+)$', name2, re.IGNORECASE)
                
                if match2_start:
                    qty2 = int(match2_start.group(1))
                    base2 = match2_start.group(2).strip()
                elif match2_end and not match2_start:
                    base2 = match2_end.group(1).strip()
                    qty2 = int(match2_end.group(2))
                else:
                    continue
                
                # Check if it's the same product family (case-insensitive)
                if product_base.lower() == base2.lower():
                    price2 = row2['precio_regular']
                    variants.append({
                        'quantity': qty2,
                        'price': price2,
                        'idx': idx2,
                        'sku': row2['sku_master']
                    })
                    unit_price = price2 / qty2 if qty2 > 0 else 0
                    unit_prices.append(unit_price)
            
            # If we found multiple variants, consolidate them
            if len(variants) > 1:
                # Calculate average unit price
                avg_unit_price = sum(unit_prices) / len(unit_prices) if unit_prices else 0
                
                # Create consolidated SKU (without quantity prefix)
                consolidated_sku = f"CHINAWOK_{product_base.upper().replace(' ', '_')}"
                
                # Mark all variants as processed
                for v in variants:
                    processed_indices.add(v['idx'])
                processed_indices.add(idx)
                
                # Use first variant as base, but update key fields
                consolidated_row = row.copy()
                consolidated_row['sku_master'] = consolidated_sku
                consolidated_row['nombre_producto'] = product_base
                consolidated_row['precio_regular'] = round(avg_unit_price, 2)  # Store unit price
                
                # Build description with variants
                variants_str = ', '.join([f'{v["quantity"]} por {v["price"]}' for v in variants])
                consolidated_row['descripcion'] = f"Precio unitario calculado. Variantes: {variants_str}"
                
                consolidated_data[consolidated_sku] = consolidated_row
                
                self.logger.info(f"Consolidated '{product_base}': {len(variants)} variants → unit price: {avg_unit_price:.2f}")
            else:
                # Single variant or no match, keep as is
                consolidated_data[row['sku_master']] = row
                processed_indices.add(idx)
        
        # Convert back to DataFrame
        result_df = pd.DataFrame(list(consolidated_data.values())).reset_index(drop=True)
        return result_df

    
    async def _scrape_all_categories(self) -> List[Dict]:
        """Scrape all category URLs"""
        all_products = []
        
        async with async_playwright() as p:
            # Launch browser
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = await context.new_page()
            
            # Scrape each category
            for category_name, category_slug in self.CATEGORIES.items():
                self.logger.info(f"Scraping category: {category_name}")
                category_url = f"{self.BASE_URL}/menu/{category_slug}"
                
                try:
                    products = await self._scrape_category(page, category_url, category_name)
                    all_products.extend(products)
                    self.logger.info(f"  Found {len(products)} products in {category_name}")
                except Exception as e:
                    self.logger.error(f"  Error scraping {category_name}: {str(e)}")
            
            await browser.close()
        
        return all_products
    
    # CSS selectors to try for product containers, in order of preference
    PRODUCT_SELECTORS = [
        "li.item.product.product-item",
        ".product-item",
        "li.product-item",
        ".product-card",
        "[data-product-id]",
    ]

    async def _scrape_category(self, page, url: str, category_name: str) -> List[Dict]:
        """Scrape a single category page"""
        products = []
        
        try:
            # Navigate to page - use domcontentloaded which is more reliable than networkidle
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            # Try each selector in order until one matches
            product_selector = None
            for selector in self.PRODUCT_SELECTORS:
                try:
                    await page.wait_for_selector(selector, timeout=15000)
                    count = await page.locator(selector).count()
                    if count > 0:
                        product_selector = selector
                        self.logger.debug(f"  Using selector '{selector}' ({count} elements)")
                        break
                except Exception:
                    continue
            
            if not product_selector:
                self.logger.warning(f"  No product elements found on {url}")
                return products
            
            # Get all product containers
            product_elements = await page.locator(product_selector).all()
            
            self.logger.info(f"  Found {len(product_elements)} product containers")
            
            # Extract data from each product
            for idx, element in enumerate(product_elements):
                try:
                    product_data = await self._extract_product_data(element, category_name)
                    if product_data:
                        products.append(product_data)
                except Exception as e:
                    self.logger.debug(f"    Error extracting product {idx}: {str(e)}")
            
        except Exception as e:
            self.logger.error(f"Error navigating to {url}: {str(e)}")
        
        return products
    
    async def _extract_product_data(self, element, category_name: str) -> Optional[Dict]:
        """Extract product data from a single product element"""
        try:
            # Get product name
            name_elem = await element.locator("strong.product-item-name a.product-item-link").first.inner_text()
            if not name_elem:
                return None
            
            product_name = name_elem.strip()
            
            # Get product URL
            url_elem = await element.locator("a.product-item-photo").first.get_attribute("href")
            product_url = url_elem if url_elem else ""
            
            # Get product ID and SKU from form
            try:
                sku_elem = await element.locator("form.tocart-form").first.get_attribute("data-product-sku")
                sku = sku_elem if sku_elem else ""
                
                product_id_elem = await element.locator("form.tocart-form").first.get_attribute("data-product-id")
                product_id = product_id_elem if product_id_elem else ""
            except Exception:
                sku = ""
                product_id = ""
            
            # Get price
            try:
                price_elem = await element.locator("span[data-price-amount]").first.get_attribute("data-price-amount")
                price = float(price_elem) if price_elem else 0.0
            except Exception:
                price = 0.0
            
            # Get description if available
            try:
                desc_elem = await element.locator("div.product-item-description-wrapper p").first.inner_text()
                description = desc_elem.strip() if desc_elem else product_name
            except Exception:
                description = product_name
            
            # Generate SKU_Master
            sku_master = self._generate_sku_master(product_name)
            
            # Extract size/tamaño if available in description
            tamaño = self._extract_size(description)
            
            return {
                "ID_PRODUCTO": product_id,
                "SKU": sku,
                "sku_master": sku_master,
                "nombre_producto": product_name,
                "descripcion": description,
                "tamaño": tamaño,
                "precio_regular": price,
                "categoria": category_name,
                "url_fuente": product_url,
                "marca": "CHINAWOK"
            }
        
        except Exception as e:
            self.logger.debug(f"Error extracting product data: {str(e)}")
            return None
    
    def _generate_sku_master(self, product_name: str) -> str:
        """
        Generate SKU_Master from product name
        Format: CHINAWOK_[PRODUCT_NAME_UPPERCASE]
        """
        # Remove accents and special characters
        normalized = unicodedata.normalize('NFD', product_name)
        without_accents = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
        
        # Convert to uppercase and replace spaces with underscores
        sku = without_accents.upper()
        sku = re.sub(r'[^A-Z0-9]+', '_', sku)
        sku = sku.strip('_')
        
        return f"CHINAWOK_{sku}"
    
    def _extract_size(self, description: str) -> str:
        """
        Extract size/tamaño from description
        Look for patterns like "1 lt", "500 ml", "3L", etc.
        """
        size_patterns = [
            r'(\d+\.?\d*\s*(?:lt|l|ml|oz|kg|gr))',
            r'(\d+\.?\d*\s*(?:litro|litros))',
        ]
        
        for pattern in size_patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return ""


if __name__ == "__main__":
    scraper = ChinawokScraper()
    df = scraper.scrape()
    print(f"\nTotal products scraped: {len(df)}")
    print(df.head())
