"""
Bembos scraper implementation
Scrapes restaurant menu from Bembos Peru
"""

import re
import unicodedata
import requests
import pandas as pd
from bs4 import BeautifulSoup
from .base_scraper import BaseScraper


class BembosScraper(BaseScraper):
    """Scraper for Bembos restaurant menu"""
    
    BASE_URL = "https://www.bembos.com.pe"
    CATEGORIES = {
        "Hamburguesas": f"{BASE_URL}/menu/hamburguesas",
        "Complementos": f"{BASE_URL}/menu/complementos",
        "Pollo": f"{BASE_URL}/menu/pollo",
        "Menu y Ensaladas": f"{BASE_URL}/menu/bembos-menus",
    }
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0 Safari/537.36"
        )
    }
    
    def __init__(self, output_dir: str = "output"):
        """Initialize Bembos scraper"""
        super().__init__(output_dir=output_dir, marca="Bembos")
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and normalize text"""
        return re.sub(r"\s+", " ", text or "").strip()
    
    @staticmethod
    def parse_price(text: str) -> float:
        """Extract price from text"""
        if not text:
            return None
        m = re.search(r"S/\s*([0-9]+(?:\.[0-9]{2})?)", text)
        return float(m.group(1)) if m else None
    
    @staticmethod
    def infer_unidad_base(categoria: str, nombre: str) -> str:
        """Infer base unit"""
        n = nombre.lower()
        if categoria == "Hamburguesas":
            return "unidad"
        if "papas" in n:
            return "porcion"
        if "ml" in n or "coca cola" in n or "inca kola" in n or "agua" in n:
            return "botella"
        return "unidad"
    
    @staticmethod
    def infer_familia_producto(categoria: str, nombre: str) -> str:
        """Infer product family"""
        n = nombre.lower()
        if categoria == "Hamburguesas":
            return "Hamburguesa"
        if "papas" in n:
            return "Papas"
        if "nuggets" in n:
            return "Pollo"
        if "sundae" in n or categoria == "Helados":
            return "Postre"
        if "cola" in n or "agua" in n:
            return "Bebida"
        return categoria
    
    @staticmethod
    def infer_subfamilia(categoria: str, nombre: str) -> str:
        """Infer product subfamily"""
        n = nombre.lower()
        if "cheese" in n:
            return "Cheese"
        if "queso tocino" in n:
            return "Queso Tocino"
        if "a lo pobre" in n:
            return "A lo Pobre"
        if "parrillera" in n:
            return "Parrillera"
        if "papas" in n:
            return "Papa Frita"
        if "nuggets" in n:
            return "Nuggets"
        return categoria
    
    @staticmethod
    def build_item_canonico(nombre: str) -> str:
        """Build canonical item name"""
        nombre = BembosScraper.clean_text(nombre)
        nombre = re.sub(r"^Hamburguesa\s+", "", nombre, flags=re.I)
        return nombre
    
    @staticmethod
    def _normalize_accents(text: str) -> str:
        """Normalize accents: Á→A, É→E, Í→I, Ó→O, Ú→U"""
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        return text
    
    @staticmethod
    def build_sku(marca: str, item_canonico: str, tamano: str = None) -> str:
        """Build SKU - omit tamano if not provided or if SIN_TAMANO"""
        import unicodedata
        
        # Normalize accents first
        item_canonico = BembosScraper._normalize_accents(item_canonico)
        if tamano:
            tamano = BembosScraper._normalize_accents(tamano)
        
        if tamano is None or tamano == "SIN_TAMANO":
            base = f"{marca}_{item_canonico}".upper()
        else:
            base = f"{marca}_{item_canonico}_{tamano}".upper()
        return re.sub(r"[^A-Z0-9]+", "_", base).strip("_")
    
    @staticmethod
    def aplica_tamanos(categoria: str, nombre: str) -> bool:
        """Check if product has sizes"""
        n = nombre.lower()
        if categoria != "Complementos":
            return True
        if "papas" in n or "nuggets" in n:
            return True
        return False
    
    def expandir_por_tamanos(
        self, 
        item_fuente: str,
        item_canonico_base: str,
        categoria_fuente: str,
        familia_producto: str,
        subfamilia: str,
        unidad_base: str,
        precio_medio: float,
        url_fuente: str,
    ) -> list:
        """Expand product by sizes"""
        filas = []
        
        if self.aplica_tamanos(categoria_fuente, item_fuente):
            tamanos = [
                ("Regular", round(precio_medio - 3, 2)),
                ("Mediana", round(precio_medio, 2)),
                ("Grande", round(precio_medio + 3, 2)),
            ]
        else:
            tamanos = [(None, round(precio_medio, 2))]
        
        for tamano, precio in tamanos:
            item_canonico = item_canonico_base if tamano is None else f"{item_canonico_base} {tamano}"
            sku_master = self.build_sku(self.marca, item_canonico_base, tamano)
            
            filas.append({
                "marca": self.marca,
                "item_fuente": item_fuente,
                "item_canonico": item_canonico,
                "sku_master": sku_master,
                "familia_producto": familia_producto,
                "subfamilia": subfamilia,
                "tamano": tamano,
                "unidad_base": unidad_base,
                "precio_regular": precio,
                "categoria_fuente": categoria_fuente,
                "url_fuente": url_fuente,
                "precio_base_fuente": precio_medio,
            })
        
        return filas
    
    def scrape_category(self, url: str, categoria_fuente: str) -> list:
        """Scrape a single category"""
        try:
            r = requests.get(url, headers=self.HEADERS, timeout=30)
            r.raise_for_status()
        except Exception as e:
            self.logger.error(f"Failed to fetch {categoria_fuente}: {e}")
            return []
        
        soup = BeautifulSoup(r.text, "html.parser")
        lines = [self.clean_text(x) for x in soup.get_text("\n").split("\n")]
        lines = [x for x in lines if x]
        
        results = []
        i = 0
        
        while i < len(lines):
            line = lines[i]
            
            if line == "Favoritos" and i >= 1:
                nombre = lines[i - 1]
                
                if nombre.lower() not in {
                    "favoritos", "mostrar", "productos", "ver todo",
                    "más relevantes", "menos relevantes", "a a z", "z to a"
                }:
                    precio_txt = lines[i + 2] if i + 2 < len(lines) else ""
                    precio_medio = self.parse_price(precio_txt)
                    
                    if precio_medio is not None:
                        item_canonico_base = self.build_item_canonico(nombre)
                        familia_producto = self.infer_familia_producto(categoria_fuente, nombre)
                        subfamilia = self.infer_subfamilia(categoria_fuente, nombre)
                        unidad_base = self.infer_unidad_base(categoria_fuente, nombre)
                        
                        filas_expandida = self.expandir_por_tamanos(
                            item_fuente=nombre,
                            item_canonico_base=item_canonico_base,
                            categoria_fuente=categoria_fuente,
                            familia_producto=familia_producto,
                            subfamilia=subfamilia,
                            unidad_base=unidad_base,
                            precio_medio=precio_medio,
                            url_fuente=url,
                        )
                        
                        results.extend(filas_expandida)
            
            i += 1
        
        return results
    
    def scrape(self) -> pd.DataFrame:
        """Execute scraping for all categories"""
        rows = []
        
        for categoria, url in self.CATEGORIES.items():
            try:
                data = self.scrape_category(url, categoria)
                rows.extend(data)
                self.logger.info(f"✓ {categoria}: {len(data)} rows")
            except Exception as e:
                self.logger.warning(f"Failed to process {categoria}: {e}")
        
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows).drop_duplicates(
            subset=["sku_master", "precio_regular", "url_fuente"]
        )
        
        return df
