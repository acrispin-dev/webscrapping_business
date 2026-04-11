"""
Popeyes Perú scraper implementation - Web scraping for Popeyes Peru menu
"""
import re
import unicodedata
import requests
import pandas as pd
from bs4 import BeautifulSoup
from .base_scraper import BaseScraper


class PopeyesScraper(BaseScraper):
    """Scraper for Popeyes Peru menu"""
    
    BASE_URL = "https://www.popeyes.com.pe"
    CATEGORIES = {
        "Tenders, Alitas y Nuggets": f"{BASE_URL}/menu/tenders-alita-y-nuggets",
        "Pollo Frito": f"{BASE_URL}/menu/pollo-frito",
        "Tostys y Sandwichs": f"{BASE_URL}/menu/sandwiches-y-tosty-rolls",
        "Complementos": f"{BASE_URL}/menu/piqueos-y-complementos",
    }
    
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0 Safari/537.36"
        )
    }
    
    def __init__(self, output_dir: str = "output"):
        """Initialize Popeyes scraper"""
        super().__init__(output_dir=output_dir, marca="Popeyes")
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and normalize text"""
        return re.sub(r"\s+", " ", text or "").strip()
    
    @staticmethod
    def parse_price(text: str) -> float:
        """Extract price from text - handles S/ prefix"""
        if not text:
            return None
        m = re.search(r"S/\s*([0-9]+(?:\.[0-9]{2})?)", text)
        if m:
            return float(m.group(1))
        return None
    
    def infer_unidad_base(self, categoria: str, nombre: str) -> str:
        """Infer base unit"""
        n = nombre.lower()
        if any(x in n for x in ["ml", "botella", "coca", "inca", "agua", "bebida"]):
            return "botella"
        if any(x in n for x in ["papas", "ensalada"]):
            return "porcion"
        if "salsa" in n:
            return "porcion"
        return "unidad"
    
    def infer_familia_producto(self, categoria: str, nombre: str) -> str:
        """Infer product family"""
        n = nombre.lower()
        if "tender" in n:
            return "Tender"
        if "nuggets" in n:
            return "Nuggets"
        if "alitas" in n:
            return "Alitas"
        if "chicharron" in n:
            return "Chicharrón"
        if "tosty" in n:
            return "Tosty"
        if "chicken roll" in n:
            return "Chicken Roll"
        if "sandwich" in n:
            return "Sandwich"
        if "papas" in n:
            return "Papas"
        if "ensalada" in n:
            return "Ensalada"
        if any(x in n for x in ["coca cola", "inca kola", "san luis"]):
            return "Bebida"
        if "salsa" in n:
            return "Salsa"
        if "pie" in n:
            return "Postre"
        if any(x in n for x in ["pollo", "pieza"]):
            return "Pollo"
        return categoria
    
    def infer_subfamilia(self, categoria: str, nombre: str) -> str:
        """Infer product subfamily"""
        n = nombre.lower()
        
        if "tender" in n:
            for qty in ["x3", "x4", "x6", "x8"]:
                if qty in n:
                    return qty
            return "Tenders"
        
        if "nuggets" in n:
            for qty in ["x4", "x8"]:
                if qty in n:
                    return qty
            return "Nuggets"
        
        if "alitas" in n:
            for qty in ["x4", "x8"]:
                if qty in n:
                    return qty
            return "Alitas"
        
        if "chicharron" in n:
            return "XL" if "xl" in n else "Pop"
        
        if "tosty" in n:
            if "crunch" in n:
                return "Crunch"
            elif "tradicional" in n:
                return "Tradicional"
            return "Tosty"
        
        if "sandwich" in n:
            if "tartara" in n or "golf" in n:
                return "Tártara Golf"
            elif "mayo" in n:
                return "Mayo Especial"
            elif "ají" in n or "aji" in n:
                return "Ají Mix"
            return "Sandwich"
        
        if "papas" in n:
            if "super" in n:
                return "Super Familiar"
            if "familiar" in n or "familiares" in n:
                return "Familiar"
            if "grandes" in n or "grande" in n:
                return "Grande"
            return "Regular"
        
        return categoria
    
    @staticmethod
    def _normalize_accents(text: str) -> str:
        """Normalize accents: Á→A, É→E, Í→I, Ó→O, Ú→U"""
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        return text
    
    def build_sku(self, marca: str, item_name: str, item_fuente: str = None) -> str:
        """Build SKU - use item_fuente as base for uniqueness when provided"""
        # Normalize accents first
        item_name = self._normalize_accents(item_name)
        if item_fuente:
            item_fuente = self._normalize_accents(item_fuente)
            # Use item_fuente to ensure uniqueness for similar products
            base = f"{marca}_{item_fuente}".upper()
        else:
            base = f"{marca}_{item_name}".upper()
        return re.sub(r"[^A-Z0-9]+", "_", base).strip("_")
    
    def extract_unitario_price(self, nombre: str, precio_total: float) -> tuple:
        """Extract unitario price from bulk price"""
        n = nombre.lower()
        
        if "tender" in n:
            if "x3" in n:
                return (round(precio_total / 3, 2), 3, "Tender Unitario")
            elif "x4" in n:
                return (round(precio_total / 4, 2), 4, "Tender Unitario")
            elif "x6" in n:
                return (round(precio_total / 6, 2), 6, "Tender Unitario")
            elif "x8" in n:
                return (round(precio_total / 8, 2), 8, "Tender Unitario")
        
        if "nuggets" in n:
            if "x4" in n:
                return (round(precio_total / 4, 2), 4, "Nuggets Unitario")
            elif "x8" in n:
                return (round(precio_total / 8, 2), 8, "Nuggets Unitario")
        
        if "alitas" in n:
            if "x4" in n:
                return (round(precio_total / 4, 2), 4, "Alitas Unitario")
            elif "x8" in n:
                return (round(precio_total / 8, 2), 8, "Alitas Unitario")
        
        if ("pieza" in n or "piezas" in n) and "pollo" in n:
            if "x2" in n:
                return (round(precio_total / 2, 2), 2, "Pieza de Pollo Unitaria")
        
        return (precio_total, 1, nombre)
    
    def scrape_category(self, url: str, categoria_fuente: str) -> list:
        """Scrape a single category"""
        try:
            r = requests.get(url, headers=self.HEADERS, timeout=30)
            r.raise_for_status()
        except Exception as e:
            self.logger.error(f"Failed to fetch {categoria_fuente}: {e}")
            return []
        
        soup = BeautifulSoup(r.text, "html.parser")
        results = []
        seen_skus = set()
        
        # Find all product containers: <div class="product-item-info">
        product_divs = soup.find_all('div', class_='product-item-info')
        self.logger.debug(f"Found {len(product_divs)} products in {categoria_fuente}")
        
        for div in product_divs:
            try:
                # Extract product name
                name_elem = div.find('a', class_='product-item-link')
                if not name_elem:
                    continue
                
                nombre = self.clean_text(name_elem.get_text())
                if not nombre or len(nombre) < 2:
                    continue
                
                # Extract price
                price_span = div.find('span', class_='price')
                if not price_span:
                    continue
                
                precio_text = self.clean_text(price_span.get_text())
                precio = self.parse_price(precio_text)
                if not precio or precio == 0:
                    continue
                
                n_lower = nombre.lower()
                
                # Process by category
                if categoria_fuente == "Tenders, Alitas y Nuggets":
                    self._process_tenders_category(nombre, n_lower, precio, url, categoria_fuente, results, seen_skus)
                
                elif categoria_fuente == "Pollo Frito":
                    self._process_pollo_frito_category(nombre, n_lower, precio, url, categoria_fuente, results, seen_skus)
                
                elif categoria_fuente == "Tostys y Sandwichs":
                    self._process_tostys_category(nombre, n_lower, precio, url, categoria_fuente, results, seen_skus)
                
                elif categoria_fuente == "Complementos":
                    self._process_complementos_category(nombre, n_lower, precio, url, categoria_fuente, results, seen_skus)
            
            except Exception as e:
                self.logger.debug(f"Error processing product: {e}")
                continue
        
        return results
    
    def _process_tenders_category(self, nombre, n_lower, precio, url, categoria, results, seen_skus):
        """Process products in Tenders category - only unitario prices"""
        # Solo añadir productos individuales base (no combos)
        
        # TENDER - Solo mostrar precio unitario (una sola fila) - usar x4 como referencia
        if "tender" in n_lower and any(x in n_lower for x in ["x3", "x4", "x6", "x8"]):
            base_sku = self.build_sku(self.marca, "TENDER")
            
            if base_sku not in seen_skus:  # Solo procesar el PRIMER Tender encontrado
                precio_unit, cantidad, nombre_unit = self.extract_unitario_price(nombre, precio)
                seen_skus.add(base_sku)
                results.append({
                    "marca": self.marca,
                    "item_fuente": nombre,
                    "item_canonico": "Tender",
                    "sku_master": base_sku,
                    "familia_producto": "Tender",
                    "subfamilia": None,
                    "tamano": None,
                    "unidad_base": "unidad",
                    "precio_regular": precio_unit,
                    "categoria_fuente": categoria,
                    "url_fuente": url,
                    "precio_base_fuente": precio,
                })
        
        # NUGGETS - Solo mostrar precio unitario (una sola fila) - usar x4 como referencia
        elif "nuggets" in n_lower and any(x in n_lower for x in ["x4", "x8"]):
            base_sku = self.build_sku(self.marca, "NUGGETS")
            
            if base_sku not in seen_skus:  # Solo procesar el PRIMER Nuggets encontrado
                precio_unit, cantidad, nombre_unit = self.extract_unitario_price(nombre, precio)
                seen_skus.add(base_sku)
                results.append({
                    "marca": self.marca,
                    "item_fuente": nombre,
                    "item_canonico": "Nuggets",
                    "sku_master": base_sku,
                    "familia_producto": "Nuggets",
                    "subfamilia": None,
                    "tamano": None,
                    "unidad_base": "unidad",
                    "precio_regular": precio_unit,
                    "categoria_fuente": categoria,
                    "url_fuente": url,
                    "precio_base_fuente": precio,
                })
        
        # ALITAS - Solo mostrar precio unitario (una sola fila) - usar x4 como referencia
        elif "alitas" in n_lower and any(x in n_lower for x in ["x4", "x8"]):
            base_sku = self.build_sku(self.marca, "ALITAS")
            
            if base_sku not in seen_skus:  # Solo procesar el PRIMER Alitas encontrado
                precio_unit, cantidad, nombre_unit = self.extract_unitario_price(nombre, precio)
                seen_skus.add(base_sku)
                results.append({
                    "marca": self.marca,
                    "item_fuente": nombre,
                    "item_canonico": "Alitas",
                    "sku_master": base_sku,
                    "familia_producto": "Alitas",
                    "subfamilia": None,
                    "tamano": None,
                    "unidad_base": "unidad",
                    "precio_regular": precio_unit,
                    "categoria_fuente": categoria,
                    "url_fuente": url,
                    "precio_base_fuente": precio,
                })
        
        # CHICHARRÓN - Mostrar ambas variantes (Pop y XL) pero una sola fila por variante
        elif "chicharron" in n_lower:
            subfamilia_tipo = "XL" if "xl" in n_lower else "Pop"
            base_sku = self.build_sku(self.marca, f"CHICHARRON_{subfamilia_tipo}")
            
            if base_sku not in seen_skus:
                seen_skus.add(base_sku)
                results.append({
                    "marca": self.marca,
                    "item_fuente": nombre,
                    "item_canonico": f"Chicharrón {subfamilia_tipo}",
                    "sku_master": base_sku,
                    "familia_producto": "Chicharrón",
                    "subfamilia": subfamilia_tipo,
                    "tamano": None,
                    "unidad_base": "unidad",
                    "precio_regular": precio,
                    "categoria_fuente": categoria,
                    "url_fuente": url,
                    "precio_base_fuente": precio,
                })
    
    def _process_tostys_category(self, nombre, n_lower, precio, url, categoria, results, seen_skus):
        """Process products in Tostys & Sandwichs category - show all variants separately"""
        
        # SANDWICH - normalize to single SKU without _X1
        if "sandwich" in n_lower and not "roll" in n_lower:
            sku = self.build_sku(self.marca, "SANDWICH")
            
            if sku not in seen_skus:
                seen_skus.add(sku)
                results.append({
                    "marca": self.marca,
                    "item_fuente": nombre,
                    "item_canonico": nombre,
                    "sku_master": sku,
                    "familia_producto": "Sandwich",
                    "subfamilia": None,
                    "tamano": None,
                    "unidad_base": "unidad",
                    "precio_regular": precio,
                    "categoria_fuente": categoria,
                    "url_fuente": url,
                    "precio_base_fuente": precio,
                })
        
        # TOSTY - normalize to single SKU without _X1
        elif "tosty" in n_lower or "tosti" in n_lower:
            sku = self.build_sku(self.marca, "TOSTY")
            
            if sku not in seen_skus:
                seen_skus.add(sku)
                results.append({
                    "marca": self.marca,
                    "item_fuente": nombre,
                    "item_canonico": nombre,
                    "sku_master": sku,
                    "familia_producto": "Tosty",
                    "subfamilia": None,
                    "tamano": None,
                    "unidad_base": "unidad",
                    "precio_regular": precio,
                    "categoria_fuente": categoria,
                    "url_fuente": url,
                    "precio_base_fuente": precio,
                })
        
        # CHICKEN ROLL - normalize to single SKU without _X1
        elif "chicken roll" in n_lower or "chicken" in n_lower and "roll" in n_lower:
            sku = self.build_sku(self.marca, "CHICKEN_ROLL")
            
            if sku not in seen_skus:
                seen_skus.add(sku)
                results.append({
                    "marca": self.marca,
                    "item_fuente": nombre,
                    "item_canonico": nombre,
                    "sku_master": sku,
                    "familia_producto": "Chicken Roll",
                    "subfamilia": None,
                    "tamano": None,
                    "unidad_base": "unidad",
                    "precio_regular": precio,
                    "categoria_fuente": categoria,
                    "url_fuente": url,
                    "precio_base_fuente": precio,
                })
    
    def _process_pollo_frito_category(self, nombre, n_lower, precio, url, categoria, results, seen_skus):
        """Process products in Pollo Frito category - show unitario prices for bulk quantities"""
        
        # PIEZA DE POLLO - Solo mostrar precio unitario (una sola fila)
        if "pieza" in n_lower and "pollo" in n_lower and any(x in n_lower for x in ["x2", "x3", "x4"]):
            base_sku = self.build_sku(self.marca, "PIEZA_POLLO")
            
            if base_sku not in seen_skus:
                precio_unit, cantidad, nombre_unit = self.extract_unitario_price(nombre, precio)
                seen_skus.add(base_sku)
                results.append({
                    "marca": self.marca,
                    "item_fuente": nombre,
                    "item_canonico": "Pieza de Pollo",
                    "sku_master": base_sku,
                    "familia_producto": "Pollo",
                    "subfamilia": None,
                    "tamano": None,
                    "unidad_base": "unidad",
                    "precio_regular": precio_unit,
                    "categoria_fuente": categoria,
                    "url_fuente": url,
                    "precio_base_fuente": precio,
                })
        
        # POLLO ENTERO - Generic handling
        elif "pollo" in n_lower and "entero" in n_lower:
            sku = self.build_sku(self.marca, "POLLO_ENTERO", nombre)
            
            if sku not in seen_skus:
                seen_skus.add(sku)
                results.append({
                    "marca": self.marca,
                    "item_fuente": nombre,
                    "item_canonico": nombre,
                    "sku_master": sku,
                    "familia_producto": "Pollo",
                    "subfamilia": "Entero",
                    "tamano": None,
                    "unidad_base": "unidad",
                    "precio_regular": precio,
                    "categoria_fuente": categoria,
                    "url_fuente": url,
                    "precio_base_fuente": precio,
                })
        
        # DEFAULT - Generic handling for other pollo frito items (Combos,  Promos, etc) - skip duplicates
        else:
            item_type = self.infer_familia_producto(categoria, nombre)
            sku = self.build_sku(self.marca, item_type, nombre)
            
            if sku not in seen_skus:
                seen_skus.add(sku)
                results.append({
                    "marca": self.marca,
                    "item_fuente": nombre,
                    "item_canonico": nombre,
                    "sku_master": sku,
                    "familia_producto": item_type,
                    "subfamilia": self.infer_subfamilia(categoria, nombre),
                    "tamano": None,
                    "unidad_base": self.infer_unidad_base(categoria, nombre),
                    "precio_regular": precio,
                    "categoria_fuente": categoria,
                    "url_fuente": url,
                    "precio_base_fuente": precio,
                })
    
    def _process_complementos_category(self, nombre, n_lower, precio, url, categoria, results, seen_skus):
        """Process products in Complementos category - show all variants by size"""
        
        # PAPAS CAJÚN - Show each size variant separately (Regular, Grande, Familiar, Super Familiar)
        if "papas" in n_lower or "papas cajun" in n_lower or "papas cajún" in n_lower:
            tamano = None
            
            if "super familiar" in n_lower or "superfamiliar" in n_lower:
                tamano = "Super Familiar"
            elif "familiar" in n_lower or "familiares" in n_lower:
                tamano = "Familiar"
            elif "grande" in n_lower or "grandes" in n_lower:
                tamano = "Grande"
            else:
                tamano = "Regular"
            
            base_sku = self.build_sku(self.marca, f"PAPAS_CAJUN_{tamano.replace(' ', '_').upper()}")
            
            if base_sku not in seen_skus:  # Only one row per size variant
                seen_skus.add(base_sku)
                results.append({
                    "marca": self.marca,
                    "item_fuente": nombre,
                    "item_canonico": f"Papas Cajún {tamano}",
                    "sku_master": base_sku,
                    "familia_producto": "Papas",
                    "subfamilia": "Cayún",
                    "tamano": tamano,
                    "unidad_base": "porcion",
                    "precio_regular": precio,
                    "categoria_fuente": categoria,
                    "url_fuente": url,
                    "precio_base_fuente": precio,
                })
        
        # CHICHARRÓN - Show each variant once (Pop, XL)
        elif "chicharron" in n_lower:
            subfamilia_tipo = "XL" if "xl" in n_lower else "Pop"
            base_sku = self.build_sku(self.marca, f"CHICHARRON_{subfamilia_tipo}")
            
            if base_sku not in seen_skus:
                seen_skus.add(base_sku)
                results.append({
                    "marca": self.marca,
                    "item_fuente": nombre,
                    "item_canonico": f"Chicharrón {subfamilia_tipo}",
                    "sku_master": base_sku,
                    "familia_producto": "Chicharrón",
                    "subfamilia": subfamilia_tipo,
                    "tamano": None,
                    "unidad_base": "unidad",
                    "precio_regular": precio,
                    "categoria_fuente": categoria,
                    "url_fuente": url,
                    "precio_base_fuente": precio,
                })
        
        # DEFAULT - Generic product handling - cada producto único
        else:
            item_type = self.infer_familia_producto(categoria, nombre)
            sku = self.build_sku(self.marca, item_type, nombre)
            
            if sku not in seen_skus:
                seen_skus.add(sku)
                results.append({
                    "marca": self.marca,
                    "item_fuente": nombre,
                    "item_canonico": nombre,
                    "sku_master": sku,
                    "familia_producto": item_type,
                    "subfamilia": self.infer_subfamilia(categoria, nombre),
                    "tamano": None,
                    "unidad_base": self.infer_unidad_base(categoria, nombre),
                    "precio_regular": precio,
                    "categoria_fuente": categoria,
                    "url_fuente": url,
                    "precio_base_fuente": precio,
                })
    
    def scrape(self) -> pd.DataFrame:
        """Scrape all categories and return combined data"""
        self.logger.info(f"Starting scrape for {self.marca}...")
        
        raw_data = []
        
        for categoria, url in self.CATEGORIES.items():
            self.logger.debug(f"Processing {categoria}")
            categoria_data = self.scrape_category(url, categoria)
            
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
