"""
Template - Ejemplo de cómo crear un nuevo scraper
Reemplaza 'TemplateComercio' con el nombre real del comercio
"""

import pandas as pd
from .base_scraper import BaseScraper


class TemplateComercioScraper(BaseScraper):
    """
    Template scraper - Reemplazar con lógica real
    
    Instrucciones:
    1. Reemplazar 'TemplateComercio' con el nombre real (ej: McDonald's)
    2. Implementar la lógica de scraping en el método scrape()
    3. Retornar DataFrame con las columnas requeridas
    """
    
    BASE_URL = "https://www.ejemplo.com"
    
    def __init__(self, output_dir: str = "output"):
        """Initialize scraper"""
        super().__init__(output_dir=output_dir, marca="TemplateComercio")
    
    def scrape(self) -> pd.DataFrame:
        """
        Implementar scraping del comercio
        
        Retorna:
            DataFrame con columnas:
            - marca: Nombre del comercio
            - item_fuente: Nombre original
            - item_canonico: Nombre estandarizado
            - sku_master: ID único
            - familia_producto: Categoría principal
            - subfamilia: Subcategoría
            - tamano: Tamaño/Variante
            - unidad_base: Unidad de medida (unidad, porcion, botella, etc)
            - precio_regular: Precio actual
            - categoria_fuente: Categoría en la fuente
            - url_fuente: URL donde se encontró
            - precio_base_fuente: Precio base original
        
        Ejemplo:
        """
        rows = []
        
        try:
            # Aquí tu lógica de scraping
            # import requests
            # from bs4 import BeautifulSoup
            
            # response = requests.get(self.BASE_URL, timeout=30)
            # soup = BeautifulSoup(response.text, 'html.parser')
            
            # Parsear elementos
            # for item in soup.find_all('producto'):
            #     rows.append({
            #         'marca': self.marca,
            #         'item_fuente': item.get('nombre'),
            #         'item_canonico': item.get('nombre'),
            #         'sku_master': item.get('id'),
            #         'familia_producto': item.get('categoria'),
            #         'subfamilia': item.get('subcategoria'),
            #         'tamano': None,
            #         'unidad_base': 'unidad',
            #         'precio_regular': float(item.get('precio', 0)),
            #         'categoria_fuente': item.get('categoria'),
            #         'url_fuente': self.BASE_URL,
            #         'precio_base_fuente': float(item.get('precio', 0)),
            #     })
            
            self.logger.info("Datos scraped correctamente")
        
        except Exception as e:
            self.logger.error(f"Error durante scraping: {e}")
        
        return pd.DataFrame(rows)


# Instrucciones para usar este template:
# 
# 1. Copiar este archivo a scrapers/nuevo_comercio_scraper.py
# 2. Reemplazar:
#    - TemplateComercio -> Nombre del comercio (McDonald's, Subway, etc)
#    - BASE_URL -> URL del comercio
#    - Implementar la lógica en scrape()
# 
# 3. Registrar en scrapers/__init__.py:
#    from .nuevo_comercio_scraper import NuevoComercioScraper
#    __all__ = ['BaseScraper', 'BembosScraper', 'NuevoComercioScraper']
# 
# 4. Registrar en main.py en get_scraper_instance():
#    scrapers_map = {
#        'BembosScraper': BembosScraper,
#        'NuevoComercioScraper': NuevoComercioScraper,
#    }
# 
# 5. Activar en config.py:
#    ACTIVE_SCRAPERS = [
#        'BembosScraper',
#        'NuevoComercioScraper',
#    ]
