"""
KFC Perú scraper implementation - Uses Playwright for JavaScript rendering
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


class KFCScraper(BaseScraper):
    """Scraper for KFC Peru menu using Playwright"""
    
    BASE_URL = "https://www.kfc.com.pe"
    CATEGORIES = {
        "Twister XL": f"{BASE_URL}/carta/twister-xl",
        "Salsas": f"{BASE_URL}/carta/salsas",
        "Sandwiches": f"{BASE_URL}/carta/sandwiches",
        "Complementos": f"{BASE_URL}/carta/complementos",
        "Postres": f"{BASE_URL}/carta/postres",
        "Bebidas": f"{BASE_URL}/carta/bebidas",
    }
    
    def __init__(self, output_dir: str = "output"):
        """Initialize KFC scraper"""
        if not HAS_PLAYWRIGHT:
            raise ImportError("Playwright is required for KFCScraper")
        super().__init__(output_dir=output_dir, marca="KFC")
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and normalize text"""
        return re.sub(r"\s+", " ", text or "").strip()
    
    @staticmethod
    def parse_price(text: str):
        """Extract price from text - handles S/ prefix, uses LAST occurrence"""
        if not text:
            return None
        # Match S/ XXXX.XX format - get ALL matches and use the last one
        matches = re.findall(r"S\/\s*(\d+(?:\.\d{2})?)", text)
        if matches:
            return float(matches[-1])  # Last price is the actual one after discounts
        return None
    
    def extract_unitario_price(self, nombre: str, precio_total: float) -> tuple:
        """Extract unitario price from bulk price"""
        n = nombre.lower()
        
        # Nuggets
        if "nuggets" in n:
            if "6" in n:
                return (round(precio_total / 6, 2), 6)
            elif "8" in n:
                return (round(precio_total / 8, 2), 8)
        
        # Hot Wings
        if "hot wings" in n or "alitas" in n:
            if "6" in n:
                return (round(precio_total / 6, 2), 6)
            elif "8" in n:
                return (round(precio_total / 8, 2), 8)
        
        # Tenders
        if "tender" in n:
            if "3" in n:
                return (round(precio_total / 3, 2), 3)
            elif "6" in n:
                return (round(precio_total / 6, 2), 6)
            elif "8" in n:
                return (round(precio_total / 8, 2), 8)
        
        return (precio_total, 1)
    
    def extract_products_from_page(self, page_url: str, categoria_fuente: str) -> list:
        """Use Playwright to load page and extract products"""
        products_data = []
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.set_viewport_size({"width": 1280, "height": 720})
                
                # Navigate to page
                self.logger.debug(f"Navigating to {page_url}")
                page.goto(page_url, wait_until="domcontentloaded", timeout=30000)
                
                # Wait for content to load
                time.sleep(3)
                
                # Get rendered HTML
                html_content = page.content()
                browser.close()
                
                # Parse HTML with BeautifulSoup
                soup = BeautifulSoup(html_content, "html.parser")
                
                # Find all links
                all_links = soup.find_all('a')
                seen_products = set()
                
                for link in all_links:
                    try:
                        link_text = self.clean_text(link.get_text())
                        href = link.get('href', '')
                        
                        # Skip navigation and short links
                        if not link_text or len(link_text) < 5 or len(link_text) > 250:
                            continue
                        
                        # Skip nav links
                        if any(skip in href.lower() for skip in ['/login', '/institucional', '/sobre', '/politicas']):
                            continue
                        
                        # Extract price
                        precio = self.parse_price(link_text)
                        if not precio or precio == 0:
                            continue
                        
                        # Extract product name
                        nombre = link_text.split('S/')[0].strip()
                        if not nombre or len(nombre) < 2:
                            continue
                        
                        # Skip duplicates
                        key = f"{nombre}|{precio}"
                        if key in seen_products:
                            continue
                        seen_products.add(key)
                        
                        n_lower = nombre.lower()
                        
                        # Process by category
                        if categoria_fuente == "Twister XL":
                            self._process_twister_category(nombre, n_lower, precio, page_url, categoria_fuente, products_data)
                        elif categoria_fuente == "Salsas":
                            self._process_salsas_category(nombre, n_lower, precio, page_url, categoria_fuente, products_data)
                        elif categoria_fuente == "Sandwiches":
                            self._process_sandwiches_category(nombre, n_lower, precio, page_url, categoria_fuente, products_data)
                        elif categoria_fuente == "Complementos":
                            self._process_complementos_category(nombre, n_lower, precio, page_url, categoria_fuente, products_data)
                        elif categoria_fuente == "Postres":
                            self._process_postres_category(nombre, n_lower, precio, page_url, categoria_fuente, products_data)
                        elif categoria_fuente == "Bebidas":
                            self._process_bebidas_category(nombre, n_lower, precio, page_url, categoria_fuente, products_data)
                    
                    except Exception as e:
                        self.logger.debug(f"Error processing link: {e}")
                        continue
        
        except Exception as e:
            self.logger.error(f"Failed to scrape {categoria_fuente}: {e}")
            return []
        
        return products_data
    
    def _process_twister_category(self, nombre, n_lower, precio, url, categoria, results):
        """Process Twister XL - only base products (not combos)"""
        if "combo" in n_lower:
            return
        
        variant = None
        if "tradicional" in n_lower:
            variant = "Tradicional"
        elif "americano" in n_lower:
            variant = "Americano"
        elif "peruano" in n_lower:
            variant = "Peruano"
        else:
            return
        
        sku = self.build_sku(self.marca, f"TWISTER_XL_{variant.upper()}")
        
        results.append({
            "marca": self.marca,
            "item_fuente": nombre,
            "item_canonico": f"Twister XL {variant}",
            "sku_master": sku,
            "familia_producto": "Twister XL",
            "subfamilia": variant,
            "tamano": None,
            "unidad_base": "unidad",
            "precio_regular": precio,
            "categoria_fuente": categoria,
            "url_fuente": url,
            "precio_base_fuente": precio,
        })
    
    def _process_salsas_category(self, nombre, n_lower, precio, url, categoria, results):
        """Process all salsas"""
        tipo_salsa = self._detect_salsa_variant(n_lower)
        sku = self.build_sku(self.marca, f"SALSA_{tipo_salsa}_FAMILIAR")
        
        results.append({
            "marca": self.marca,
            "item_fuente": nombre,
            "item_canonico": f"Salsa {tipo_salsa.title().replace('_', ' ')} Familiar",
            "sku_master": sku,
            "familia_producto": "Salsa",
            "subfamilia": tipo_salsa,
            "tamano": "Familiar",
            "unidad_base": "porcion",
            "precio_regular": precio,
            "categoria_fuente": categoria,
            "url_fuente": url,
            "precio_base_fuente": precio,
        })
    
    def _process_sandwiches_category(self, nombre, n_lower, precio, url, categoria, results):
        """Process Krunchy sandwiches"""
        variant = None
        presentacion = "COMBO" if ("combo" in n_lower or "big box" in n_lower) else "SOLO"
        
        if "americano" in n_lower and "krunchy" in n_lower:
            variant = "Americano"
        elif "bbq" in n_lower and "krunchy" in n_lower:
            variant = "BBQ"
        elif "meltz" in n_lower and "krunchy" in n_lower:
            variant = "Meltz"
        
        if variant:
            sku = self.build_sku(self.marca, f"KRUNCHY_{variant.upper()}_{presentacion}")
            
            results.append({
                "marca": self.marca,
                "item_fuente": nombre,
                "item_canonico": f"Krunchy {variant} {presentacion.title()}",
                "sku_master": sku,
                "familia_producto": "Sandwich",
                "subfamilia": f"{variant}_{presentacion}",
                "tamano": None,
                "unidad_base": "unidad",
                "precio_regular": precio,
                "categoria_fuente": categoria,
                "url_fuente": url,
                "precio_base_fuente": precio,
            })
    
    def _process_complementos_category(self, nombre, n_lower, precio, url, categoria, results):
        """Process Complementos with unitario pricing"""
        
        existing_skus = {row.get("sku_master") for row in results}

        # NUGGETS (single line)
        if "nuggets" in n_lower and any(x in n_lower for x in ["6", "8"]):
            precio_unit, _ = self.extract_unitario_price(nombre, precio)
            sku = self.build_sku(self.marca, "NUGGETS")
            if sku not in existing_skus:
                results.append({
                    "marca": self.marca,
                    "item_fuente": nombre,
                    "item_canonico": "Nuggets Unitario",
                    "sku_master": sku,
                    "familia_producto": "Nuggets",
                    "subfamilia": "Unitario",
                    "tamano": None,
                    "unidad_base": "unidad",
                    "precio_regular": precio_unit,
                    "categoria_fuente": categoria,
                    "url_fuente": url,
                    "precio_base_fuente": precio,
                })
        
        # HOT WINGS
        elif ("hot wing" in n_lower or "alita" in n_lower) and any(x in n_lower for x in ["6", "8"]):
            precio_unit, _ = self.extract_unitario_price(nombre, precio)
            sku = self.build_sku(self.marca, "HOT_WINGS")
            
            results.append({
                "marca": self.marca,
                "item_fuente": nombre,
                "item_canonico": "Hot Wings Unitario",
                "sku_master": sku,
                "familia_producto": "Hot Wings",
                "subfamilia": "Unitario",
                "tamano": None,
                "unidad_base": "unidad",
                "precio_regular": precio_unit,
                "categoria_fuente": categoria,
                "url_fuente": url,
                "precio_base_fuente": precio,
            })
        
        # TENDERS (single line)
        elif "tender" in n_lower and any(x in n_lower for x in ["3", "6", "8"]):
            precio_unit, _ = self.extract_unitario_price(nombre, precio)
            sku = self.build_sku(self.marca, "TENDERS")
            if sku not in existing_skus:
                results.append({
                    "marca": self.marca,
                    "item_fuente": nombre,
                    "item_canonico": "Tenders Unitario",
                    "sku_master": sku,
                    "familia_producto": "Tenders",
                    "subfamilia": "Unitario",
                    "tamano": None,
                    "unidad_base": "unidad",
                    "precio_regular": precio_unit,
                    "categoria_fuente": categoria,
                    "url_fuente": url,
                    "precio_base_fuente": precio,
                })
        
        # PIEZAS DE POLLO
        elif ("pieza" in n_lower or "piezas" in n_lower) and "pollo" in n_lower:
            precio_unit, _ = self.extract_unitario_price(nombre, precio)
            sku = self.build_sku(self.marca, "PIEZA_POLLO")
            
            results.append({
                "marca": self.marca,
                "item_fuente": nombre,
                "item_canonico": "Pieza de Pollo Unitaria",
                "sku_master": sku,
                "familia_producto": "Pollo",
                "subfamilia": "Pieza Unitaria",
                "tamano": None,
                "unidad_base": "unidad",
                "precio_regular": precio_unit,
                "categoria_fuente": categoria,
                "url_fuente": url,
                "precio_base_fuente": precio,
            })
        
        # OTHER ITEMS
        else:
            tamano = self._detect_size(n_lower)
            familia = self._infer_familia(n_lower)
            subfamilia = self._detect_complement_subfamily(familia)
            # Build SKU without SIN_TAMANO placeholder
            if tamano:
                size_tag = tamano.upper().replace(" ", "_")
                sku = self.build_sku(self.marca, f"{familia.upper()}_{size_tag}")
            else:
                sku = self.build_sku(self.marca, familia.upper())
            
            results.append({
                "marca": self.marca,
                "item_fuente": nombre,
                "item_canonico": f"{familia} {subfamilia or ''} {tamano or ''}".strip(),
                "sku_master": sku,
                "familia_producto": familia,
                "subfamilia": subfamilia,
                "tamano": tamano,
                "unidad_base": "porcion" if tamano else "unidad",
                "precio_regular": precio,
                "categoria_fuente": categoria,
                "url_fuente": url,
                "precio_base_fuente": precio,
            })
    
    def _process_postres_category(self, nombre, n_lower, precio, url, categoria, results):
        """Process all postres"""
        tipo_postre = self._detect_postre_variant(n_lower)
        sku = self.build_sku(self.marca, f"POSTRE_{tipo_postre}")
        
        results.append({
            "marca": self.marca,
            "item_fuente": nombre,
            "item_canonico": tipo_postre.title().replace("_", " "),
            "sku_master": sku,
            "familia_producto": "Postre",
            "subfamilia": tipo_postre,
            "tamano": None,
            "unidad_base": "unidad",
            "precio_regular": precio,
            "categoria_fuente": categoria,
            "url_fuente": url,
            "precio_base_fuente": precio,
        })
    
    def _process_bebidas_category(self, nombre, n_lower, precio, url, categoria, results):
        """Process all bebidas"""
        tamano = self._detect_size(n_lower)
        bebida_base = self._detect_bebida_variant(n_lower)
        # Build SKU: include size only if present (keep _1L, _1_5L, _2_25L but not SIN_TAMANO or _500ML)
        if tamano and tamano.upper() not in ["SIN_TAMANO", "500ML"]:
            size_tag = tamano.upper().replace(" ", "_")
            sku = self.build_sku(self.marca, f"BEBIDA_{bebida_base}_{size_tag}")
        else:
            sku = self.build_sku(self.marca, f"BEBIDA_{bebida_base}")
        
        results.append({
            "marca": self.marca,
            "item_fuente": nombre,
            "item_canonico": f"{bebida_base.title().replace('_', ' ')} {tamano or ''}".strip(),
            "sku_master": sku,
            "familia_producto": "Bebida",
            "subfamilia": bebida_base,
            "tamano": tamano,
            "unidad_base": "botella" if tamano else "unidad",
            "precio_regular": precio,
            "categoria_fuente": categoria,
            "url_fuente": url,
            "precio_base_fuente": precio,
        })

    def _detect_size(self, n_lower: str):
        """Detect common product sizes from raw name."""
        if "super familiar" in n_lower or "superfamiliar" in n_lower:
            return "Super Familiar"
        if "familiar" in n_lower:
            return "Familiar"
        if "2.25" in n_lower or "2,25" in n_lower:
            return "2.25L"
        if "1.5" in n_lower:
            return "1.5L"
        if "1l" in n_lower or " 1 l" in n_lower:
            return "1L"
        if "750" in n_lower:
            return "750ml"
        if "625" in n_lower:
            return "625ml"
        if "500" in n_lower:
            return "500ml"
        if "regular" in n_lower:
            return "Regular"
        if "personal" in n_lower:
            return "Personal"
        return None

    def _detect_salsa_variant(self, n_lower: str) -> str:
        if "aji" in n_lower or "ají" in n_lower:
            return "AJI_CASA"
        if "tartara" in n_lower or "tártara" in n_lower:
            return "TARTARA_PERUANA"
        if "bbq" in n_lower:
            return "BBQ"
        if "secreta" in n_lower:
            return "SALSA_SECRETA"
        if "honey" in n_lower and "mustard" in n_lower:
            return "HONEY_MUSTARD"
        return "OTRA"

    def _detect_postre_variant(self, n_lower: str) -> str:
        if "tres leches" in n_lower:
            return "TRES_LECHES"
        if "torta trufada" in n_lower:
            return "TORTA_TRUFADA"
        if "pie de manzana" in n_lower:
            return "PIE_MANZANA"
        if "pie de dulce de leche" in n_lower:
            return "PIE_DULCE_LECHE"
        if "galleta" in n_lower and "avena" in n_lower:
            return "GALLETA_AVENA"
        return "POSTRE_OTRO"

    def _detect_bebida_variant(self, n_lower: str) -> str:
        if "inca" in n_lower and "zero" in n_lower:
            return "INCA_KOLA_ZERO"
        if "inca" in n_lower:
            return "INCA_KOLA"
        if "coca" in n_lower and "zero" in n_lower:
            return "COCA_COLA_ZERO"
        if "coca" in n_lower:
            return "COCA_COLA"
        if "fanta" in n_lower:
            return "FANTA"
        if "sprite" in n_lower:
            return "SPRITE"
        if "agua saborizada" in n_lower or "manzana" in n_lower:
            return "AGUA_SABORIZADA_MANZANA"
        if "san luis" in n_lower:
            return "AGUA_SAN_LUIS"
        return "BEBIDA_OTRA"

    def _detect_complement_subfamily(self, familia: str):
        if familia == "Papas":
            return "Cajun"
        if familia == "Ensalada":
            return "Col"
        if familia == "PopCorn Chicken":
            return "Popcorn"
        if familia == "Pure":
            return "Gravy"
        return None
    
    def _infer_familia(self, n_lower: str) -> str:
        """Infer product family"""
        if "papa" in n_lower or "papas" in n_lower:
            return "Papas"
        if "ensalada" in n_lower:
            return "Ensalada"
        if "popcorn" in n_lower:
            return "PopCorn Chicken"
        if "pur" in n_lower or "pure" in n_lower:
            return "Pure"
        if "sopa" in n_lower or "caldo" in n_lower:
            return "Sopa"
        return "Complemento"
    
    @staticmethod
    def _normalize_accents(text: str) -> str:
        """Normalize accents: Á→A, É→E, Í→I, Ó→O, Ú→U"""
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        return text
    
    def build_sku(self, marca: str, item_name: str, item_fuente: str = None) -> str:
        """Build SKU"""
        # Normalize accents first
        item_name = self._normalize_accents(item_name)
        if item_fuente:
            item_fuente = self._normalize_accents(item_fuente)
        
        if item_fuente and "UNITARIO" not in item_name:
            base = f"{marca}_{item_fuente}".upper()
        else:
            base = f"{marca}_{item_name}".upper()
        return re.sub(r"[^A-Z0-9]+", "_", base).strip("_")
    
    def scrape(self) -> pd.DataFrame:
        """Scrape all categories and return combined data"""
        self.logger.info(f"Starting scrape for {self.marca}...")
        
        raw_data = []
        
        for categoria, url in self.CATEGORIES.items():
            self.logger.debug(f"Processing {categoria}")
            categoria_data = self.extract_products_from_page(url, categoria)
            
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
