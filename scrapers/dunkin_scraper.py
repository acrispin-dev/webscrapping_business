"""
Dunkin Perú scraper implementation - Uses Playwright for JavaScript rendering
Focuses on extracting donuts with unit pricing
"""
import re
import unicodedata
import time
import pandas as pd
from bs4 import BeautifulSoup
from .base_scraper import BaseScraper

try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False


class DunkinScraper(BaseScraper):
    """Scraper for Dunkin Peru menu using Playwright"""
    
    BASE_URL = "https://www.dunkin.pe/menu"
    CATEGORIES = {
        "Donuts": "/donuts",
        "Bebidas Frías": "/frias/frozen",
        "Cafe Dunkin": "/calientes/cafe-dunkin",
        "Expreso Caliente": "/calientes/expreso-caliente",
        "Otras bebidas Calientes": "/calientes/otras-bebidas-calientes",
        "Novedades": "/novedades",
        "Sandwichs": "/sandwichs",
        "Otros Bakery": "/otros/bakery",
    }
    
    def __init__(self, output_dir: str = "output"):
        """Initialize Dunkin scraper"""
        if not HAS_PLAYWRIGHT:
            raise ImportError("Playwright is required for DunkinScraper")
        super().__init__(output_dir=output_dir, marca="Dunkin")
        self.browser = None
        self.playwright = None
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and normalize text"""
        return re.sub(r"\s+", " ", text or "").strip()
    
    @staticmethod
    def parse_price(text: str):
        """Extract price from text - handles S/ prefix"""
        if not text:
            return None
        # Match S/ XXXX.XX format
        matches = re.findall(r"S/\s*(\d+(?:\.\d{2})?)", text)
        if matches:
            return float(matches[-1])  # Last price
        return None
    
    def extract_unitario_price(self, nombre: str, precio_total: float) -> tuple:
        """Extract unitario price from bulk price - mainly for donuts"""
        n = nombre.lower()
        
        # Docena de donas (12 units)
        if "docena" in n or "12" in n:
            return (round(precio_total / 12, 2), 12)
        
        # Media docena (6 units)
        if "media docena" in n or "6" in n:
            return (round(precio_total / 6, 2), 6)
        
        # Pack de 3
        if "pack" in n and "3" in n:
            return (round(precio_total / 3, 2), 3)
        
        # Default: single unit
        return (precio_total, 1)
    
    def extract_products_from_page(self, categoria_nombre: str, categoria_ruta: str) -> list:
        """Use Playwright to load page and extract products - reuses browser"""
        products_data = []
        
        try:
            # Initialize browser if not already done
            if not self.browser:
                self.playwright = sync_playwright().start()
                self.browser = self.playwright.chromium.launch(headless=True)
            
            page = self.browser.new_page()
            page.set_viewport_size({"width": 1280, "height": 720})
            
            try:
                # Navigate to page with new URL structure
                page_url = f"{self.BASE_URL}{categoria_ruta}"
                self.logger.debug(f"Navigating to {page_url}")
                
                try:
                    page.goto(page_url, wait_until="networkidle", timeout=25000)
                except:
                    page.goto(page_url, wait_until="domcontentloaded", timeout=25000)
                
                # Wait for content to load
                time.sleep(2)
                
                # Scroll to load lazy-loaded content
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1)
                
                # Get rendered HTML
                html_content = page.content()
                
                self.logger.debug(f"HTML content length: {len(html_content)} bytes")
                
                # Parse HTML with BeautifulSoup
                soup = BeautifulSoup(html_content, "html.parser")
                
                # Try extraction strategies
                products_data = self._extract_products_strategy1(soup, categoria_nombre, page_url)
                if not products_data:
                    products_data = self._extract_products_strategy2(soup, categoria_nombre, page_url)
                if not products_data:
                    products_data = self._extract_products_strategy3(soup, categoria_nombre, page_url)
                
                if not products_data:
                    self.logger.warning(f"No products found for {categoria_nombre}")
                    products_data = []
            
            finally:
                page.close()
                # Delay to avoid saturating server
                time.sleep(1)
        
        except Exception as e:
            self.logger.error(f"Failed to scrape {categoria_nombre}: {e}", exc_info=True)
            products_data = []
        
        return products_data
    
    def _extract_products_strategy1(self, soup, categoria_nombre, page_url):
        """Strategy 1: Extract from product title containers (strong.product.product-item-name > a)"""
        products_data = []
        seen_products = set()
        
        # Look for product titles: <strong class="product name product-item-name"><a>Name</a></strong>
        product_titles = soup.find_all('strong', class_='product')
        self.logger.debug(f"Strategy 1: Found {len(product_titles)} product title containers")
        
        for container in product_titles:
            try:
                # Get the link inside the strong tag
                link = container.find('a', class_='product-item-link')
                if not link:
                    continue
                
                # Extract product name from link text
                nombre = self.clean_text(link.get_text())
                if not nombre or len(nombre) < 2 or len(nombre) > 150:
                    continue
                
                # Find parent container to get price
                parent_container = container.find_parent('li', class_='product')
                if not parent_container:
                    parent_container = container.find_parent('div', class_=re.compile(r'product-item|product-card|product'))
                
                if not parent_container:
                    continue
                
                # Extract price from parent container
                container_text = self.clean_text(parent_container.get_text())
                precio = self.parse_price(container_text)
                if not precio or precio == 0:
                    continue
                
                # Skip duplicates
                key = f"{nombre}|{precio}"
                if key in seen_products:
                    continue
                seen_products.add(key)
                
                n_lower = nombre.lower()
                self.logger.debug(f"Found product (strategy1): {nombre} - S/ {precio}")
                
                # Process product with real name
                self._process_product(nombre, n_lower, precio, page_url, categoria_nombre, products_data)
            
            except Exception as e:
                self.logger.debug(f"Error in strategy1: {e}")
                continue
        
        return products_data if products_data else None
    
    def _extract_products_strategy2(self, soup, categoria_nombre, page_url):
        """Strategy 2: Extract from divs/spans containing prices"""
        products_data = []
        seen_products = set()
        
        elements = soup.find_all(['div', 'span', 'p'])
        self.logger.debug(f"Strategy 2: Searching in {len(elements)} elements")
        
        for elem in elements:
            try:
                text = self.clean_text(elem.get_text())
                
                if 'S/' not in text or len(text) < 5:
                    continue
                
                precio = self.parse_price(text)
                if not precio or precio == 0:
                    continue
                
                nombre = text.split('S/')[0].strip()
                if not nombre or len(nombre) < 2 or len(nombre) > 250:
                    continue
                
                key = f"{nombre}|{precio}"
                if key in seen_products:
                    continue
                seen_products.add(key)
                
                n_lower = nombre.lower()
                self.logger.debug(f"Found product (strategy2): {nombre} - S/ {precio}")
                
                self._process_product(nombre, n_lower, precio, page_url, categoria_nombre, products_data)
            
            except Exception as e:
                self.logger.debug(f"Error in strategy2: {e}")
                continue
        
        return products_data if products_data else None
    
    def _extract_products_strategy3(self, soup, categoria_nombre, page_url):
        """Strategy 3: Extract from all text with regex pattern"""
        products_data = []
        seen_products = set()
        
        all_text = soup.get_text()
        self.logger.debug(f"Strategy 3: Total page text: {len(all_text)} chars")
        
        # Look for pattern: "Nombre Producto S/ PRECIO"
        pattern = r'([^\n]{5,200}?)\s+S/\s*(\d+(?:\.\d{2})?)'
        matches = re.finditer(pattern, all_text)
        
        for match in matches:
            try:
                nombre = self.clean_text(match.group(1))
                precio_str = match.group(2)
                precio = float(precio_str)
                
                if precio == 0 or len(nombre) < 2:
                    continue
                
                key = f"{nombre}|{precio}"
                if key in seen_products:
                    continue
                seen_products.add(key)
                
                n_lower = nombre.lower()
                self.logger.debug(f"Found product (strategy3): {nombre} - S/ {precio}")
                
                self._process_product(nombre, n_lower, precio, page_url, categoria_nombre, products_data)
            
            except Exception as e:
                self.logger.debug(f"Error in strategy3: {e}")
                continue
        
        return products_data if products_data else None
    
    def _process_product(self, nombre, n_lower, precio, url, categoria, results):
        """Process product based on category"""
        
        # DONAS - Extract unit price
        if categoria == "Donuts":
            self._process_donuts(nombre, n_lower, precio, url, categoria, results)
        # BEBIDAS FRÍAS
        elif categoria == "Bebidas Frías":
            self._process_bebidas_frias(nombre, n_lower, precio, url, categoria, results)
        # BEBIDAS CALIENTES
        elif categoria == "Bebidas Calientes":
            self._process_bebidas_calientes(nombre, n_lower, precio, url, categoria, results)
        # SANDWICHES
        elif categoria == "Sandwiches":
            self._process_sandwiches(nombre, n_lower, precio, url, categoria, results)
        # NOVEDADES
        elif categoria == "Novedades":
            self._process_novedades(nombre, n_lower, precio, url, categoria, results)
        # OTROS
        elif categoria == "Otros":
            self._process_otros(nombre, n_lower, precio, url, categoria, results)
    
    def _process_donuts(self, nombre, n_lower, precio, url, categoria, results):
        """Process donuts - calculate unit price"""
        existing_skus = {row.get("sku_master") for row in results}
        
        # Check if it's a bulk package
        precio_unit, cantidad = self.extract_unitario_price(nombre, precio)
        
        # Determine dona type
        dona_tipo = self._detect_dona_type(n_lower)
        
        # Build SKU using product name (not generic type)
        sku = self.build_sku(self.marca, f"DONA_{nombre}" + (f"_X{cantidad}" if cantidad > 1 else ""))
        
        if sku not in existing_skus:
            results.append({
                "marca": self.marca,
                "item_fuente": nombre,
                "item_canonico": nombre + (f" (x{cantidad})" if cantidad > 1 else ""),
                "sku_master": sku,
                "familia_producto": "Dona",
                "subfamilia": dona_tipo,
                "tamano": None,
                "unidad_base": "unidad",
                "precio_regular": precio_unit,
                "categoria_fuente": categoria,
                "url_fuente": url,
                "precio_base_fuente": precio,
            })
    
    def _process_bebidas_frias(self, nombre, n_lower, precio, url, categoria, results):
        """Process cold beverages"""
        tamano = self._detect_size(n_lower)
        bebida_tipo = self._detect_bebida_type(n_lower)
        
        # Build SKU using product name (unique identifier)
        sku = self.build_sku(self.marca, f"BEBIDA_FRIA_{nombre}")
        
        results.append({
            "marca": self.marca,
            "item_fuente": nombre,
            "item_canonico": nombre,
            "sku_master": sku,
            "familia_producto": "Bebida Fría",
            "subfamilia": bebida_tipo,
            "tamano": tamano,
            "unidad_base": "bebida",
            "precio_regular": precio,
            "categoria_fuente": categoria,
            "url_fuente": url,
            "precio_base_fuente": precio,
        })
    
    def _process_bebidas_calientes(self, nombre, n_lower, precio, url, categoria, results):
        """Process hot beverages"""
        tamano = self._detect_size(n_lower)
        bebida_tipo = self._detect_bebida_type(n_lower)
        
        # Build SKU using product name (unique identifier)
        sku = self.build_sku(self.marca, f"BEBIDA_CALIENTE_{nombre}")
        
        results.append({
            "marca": self.marca,
            "item_fuente": nombre,
            "item_canonico": nombre,
            "sku_master": sku,
            "familia_producto": "Bebida Caliente",
            "subfamilia": bebida_tipo,
            "tamano": tamano,
            "unidad_base": "bebida",
            "precio_regular": precio,
            "categoria_fuente": categoria,
            "url_fuente": url,
            "precio_base_fuente": precio,
        })
    
    def _process_sandwiches(self, nombre, n_lower, precio, url, categoria, results):
        """Process sandwiches"""
        sandwich_tipo = self._detect_sandwich_type(n_lower)
        
        # Build SKU using product name (unique identifier)
        sku = self.build_sku(self.marca, f"SANDWICH_{nombre}")
        
        results.append({
            "marca": self.marca,
            "item_fuente": nombre,
            "item_canonico": nombre,
            "sku_master": sku,
            "familia_producto": "Sandwich",
            "subfamilia": sandwich_tipo,
            "tamano": None,
            "unidad_base": "unidad",
            "precio_regular": precio,
            "categoria_fuente": categoria,
            "url_fuente": url,
            "precio_base_fuente": precio,
        })
    
    def _process_novedades(self, nombre, n_lower, precio, url, categoria, results):
        """Process new/special products"""
        # Try to categorize as dona or bebida first
        if "dona" in n_lower:
            self._process_donuts(nombre, n_lower, precio, url, categoria, results)
        elif "bebida" in n_lower or "cafe" in n_lower or "frappé" in n_lower:
            self._process_bebidas_frias(nombre, n_lower, precio, url, "Novedades - Bebida", results)
        else:
            # Generic novelty - use product name as SKU identifier
            sku = self.build_sku(self.marca, f"NOVEDAD_{nombre}")
            
            results.append({
                "marca": self.marca,
                "item_fuente": nombre,
                "item_canonico": nombre,
                "sku_master": sku,
                "familia_producto": "Novedad",
                "subfamilia": "Especial",
                "tamano": None,
                "unidad_base": "unidad",
                "precio_regular": precio,
                "categoria_fuente": categoria,
                "url_fuente": url,
                "precio_base_fuente": precio,
            })
    
    def _process_otros(self, nombre, n_lower, precio, url, categoria, results):
        """Process other items"""
        sku = self.build_sku(self.marca, "OTRO")
        
        results.append({
            "marca": self.marca,
            "item_fuente": nombre,
            "item_canonico": nombre,
            "sku_master": sku,
            "familia_producto": "Otro",
            "subfamilia": "General",
            "tamano": None,
            "unidad_base": "unidad",
            "precio_regular": precio,
            "categoria_fuente": categoria,
            "url_fuente": url,
            "precio_base_fuente": precio,
        })
    
    def _detect_dona_type(self, n_lower: str) -> str:
        """Detect dona type"""
        if "glaseada" in n_lower or "glazed" in n_lower:
            return "GLASEADA"
        if "chocolate" in n_lower:
            return "CHOCOLATE"
        if "rellena" in n_lower or "filled" in n_lower:
            return "RELLENA"
        if "azúcar" in n_lower or "sugar" in n_lower:
            return "AZUCAR"
        if "canela" in n_lower or "cinnamon" in n_lower:
            return "CANELA"
        if "fruta" in n_lower or "fruit" in n_lower:
            return "FRUTA"
        if "boston" in n_lower:
            return "BOSTON"
        if "twist" in n_lower:
            return "TWIST"
        if "munchkin" in n_lower:
            return "MUNCHKIN"
        return "MIXTA"
    
    def _detect_bebida_type(self, n_lower: str) -> str:
        """Detect beverage type"""
        if "cafe" in n_lower or "coffee" in n_lower:
            return "CAFE"
        if "cappuccino" in n_lower:
            return "CAPPUCCINO"
        if "latte" in n_lower:
            return "LATTE"
        if "frappé" in n_lower or "frappe" in n_lower:
            return "FRAPPE"
        if "chocolate" in n_lower:
            return "CHOCOLATE"
        if "jugo" in n_lower or "juice" in n_lower:
            return "JUGO"
        if "te" in n_lower or "tea" in n_lower:
            return "TE"
        if "smoothie" in n_lower:
            return "SMOOTHIE"
        return "BEBIDA"
    
    def _detect_sandwich_type(self, n_lower: str) -> str:
        """Detect sandwich type"""
        if "jamon" in n_lower or "ham" in n_lower:
            return "JAMON"
        if "pavo" in n_lower or "turkey" in n_lower:
            return "PAVO"
        if "pollo" in n_lower or "chicken" in n_lower:
            return "POLLO"
        if "queso" in n_lower or "cheese" in n_lower:
            return "QUESO"
        if "vegano" in n_lower or "vegan" in n_lower:
            return "VEGANO"
        return "MIXTO"
    
    def _detect_size(self, n_lower: str) -> str:
        """Detect size"""
        if "grande" in n_lower or "large" in n_lower or "lg" in n_lower:
            return "Grande"
        if "pequeño" in n_lower or "small" in n_lower or "sm" in n_lower:
            return "Pequeño"
        if "mediano" in n_lower or "medium" in n_lower or "md" in n_lower:
            return "Mediano"
        return None
    
    @staticmethod
    def _normalize_accents(text: str) -> str:
        """Normalize accents: Á→A, É→E, etc."""
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        return text
    
    def build_sku(self, marca: str, item_name: str) -> str:
        """Build SKU following established methodology"""
        item_name = self._normalize_accents(item_name)
        base = f"{marca}_{item_name}".upper()
        return re.sub(r"[^A-Z0-9]+", "_", base).strip("_")
    
    def scrape(self) -> pd.DataFrame:
        """Scrape all categories and return combined data"""
        self.logger.info(f"Starting scrape for {self.marca}...")
        
        raw_data = []
        
        try:
            for categoria, hash_anchor in self.CATEGORIES.items():
                self.logger.debug(f"Processing {categoria}")
                categoria_data = self.extract_products_from_page(categoria, hash_anchor)
                
                # Ensure categoria_data is always a list
                if categoria_data is None:
                    categoria_data = []
                
                if categoria_data:
                    self.logger.info(f"✓ {categoria}: {len(categoria_data)} rows")
                else:
                    self.logger.info(f"✓ {categoria}: 0 rows")
                
                raw_data.extend(categoria_data)
            
            if not raw_data:
                self.logger.warning(f"No data retrieved for {self.marca}")
                return pd.DataFrame()
            
            df = pd.DataFrame(raw_data)
            self.logger.info(f"Raw data: {len(df)} rows")
            
            return df
        
        finally:
            # Close browser to avoid saturating resources
            self._close_browser()
    
    def _close_browser(self):
        """Close the Playwright browser and context"""
        try:
            if self.browser:
                self.browser.close()
                self.browser = None
            if self.playwright:
                self.playwright.stop()
                self.playwright = None
            self.logger.debug("Browser closed successfully")
        except Exception as e:
            self.logger.debug(f"Error closing browser: {e}")
