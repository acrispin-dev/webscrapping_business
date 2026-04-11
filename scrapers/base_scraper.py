"""
Base class for web scrapers
Provides common structure and methods for all commerce scrapers
"""

from abc import ABC, abstractmethod
import re
import unicodedata
import pandas as pd
import logging
from typing import List, Dict, Optional
from pathlib import Path


class BaseScraper(ABC):
    """Abstract base class for scrapers"""
    
    def __init__(self, output_dir: str = "output", marca: str = ""):
        """
        Initialize scraper
        
        Args:
            output_dir: Directory to save output files
            marca: Brand/Commerce name for the scraper
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.marca = marca
        self.logger = self._setup_logger()
        self.df_raw = pd.DataFrame()
        self.df_maestro = pd.DataFrame()
        self.df_precios = pd.DataFrame()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger for the scraper"""
        logger = logging.getLogger(self.marca or self.__class__.__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(f'[{self.marca}] %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger
    
    @abstractmethod
    def scrape(self) -> pd.DataFrame:
        """
        Main scraping method. Must be implemented by subclasses
        Should return raw data DataFrame
        """
        pass
    
    def process_data(self) -> None:
        """
        Process raw data into maestro and precios tables
        Subclasses can override for custom processing
        """
        if self.df_raw.empty:
            self.logger.warning("No raw data to process")
            return

        if 'sku_comparable' not in self.df_raw.columns:
            self.df_raw['sku_comparable'] = self.df_raw.apply(self.build_comparable_sku, axis=1)
        
        # Generate maestro (unique products)
        maestro_cols = [col for col in self.df_raw.columns 
                       if col not in ['precio_regular', 'fecha_inicio', 'fecha_fin', 
                                     'fuente_precio', 'url_fuente', 'precio_base_fuente']]
        self.df_maestro = self.df_raw[maestro_cols].drop_duplicates(
            subset=['sku_master']
        ).reset_index(drop=True)
        
        # Generate precios (price history)
        fecha_captura = pd.Timestamp.today().normalize()
        precios_cols = ['sku_master', 'sku_comparable', 'precio_regular', 'precio_base_fuente', 'url_fuente']
        self.df_precios = self.df_raw[precios_cols].drop_duplicates(
            subset=['sku_master', 'precio_regular', 'url_fuente']
        ).assign(
            fecha_inicio=fecha_captura,
            fecha_fin=pd.NaT,
            fuente_precio=f"web_{self.marca.lower()}"
        )[['sku_master', 'sku_comparable', 'fecha_inicio', 'fecha_fin', 'precio_regular', 
           'fuente_precio', 'url_fuente', 'precio_base_fuente']
        ].reset_index(drop=True)

    @staticmethod
    def _normalize_sku_part(value: str) -> str:
        """Normalize any string into uppercase ASCII token for comparable SKUs."""
        if value is None:
            return ""
        value = str(value).strip().upper()
        if not value:
            return ""
        value = unicodedata.normalize("NFKD", value)
        value = "".join(ch for ch in value if not unicodedata.combining(ch))
        value = re.sub(r"[^A-Z0-9]+", "_", value)
        return value.strip("_")
    
    @staticmethod
    def _clean_sku_for_comparable(sku_master: str) -> str:
        """Remove brand prefix and technical suffixes from sku_master for comparable SKU."""
        # Remove brand prefix (BEMBOS_, POPEYES_, KFC_, PIZZAHUT_)
        sku = re.sub(r"^(BEMBOS|POPEYES|KFC|PIZZAHUT)_", "", sku_master)
        
        # Remove brand prefix and technical suffixes
        sku = re.sub(r"_SIN_TAMANO$", "", sku)
        sku = re.sub(r"_UNITARIO_BASE$", "", sku)
        sku = re.sub(r"_BASE$", "", sku)
        sku = re.sub(r"_X1$", "", sku)
        sku = re.sub(r"_REF_X6$", "", sku)
        sku = re.sub(r"_3_OZ$", "", sku)
        
        # Remove flavor/spec suffixes that start with _SAB, _SIN_AZ, etc.
        sku = re.sub(r"_SABOR_ORIGINAL_500_ML$", "", sku)
        sku = re.sub(r"_SABOR_ORIGINAL_500ML$", "", sku)
        sku = re.sub(r"_SABOR_ORIGINAL$", "", sku)
        sku = re.sub(r"_SIN_AZ_CAR_500_ML$", "", sku)
        sku = re.sub(r"_SIN_AZ_CAR_500ML$", "", sku)
        sku = re.sub(r"_S_AZ_CAR_500ML$", "", sku)
        sku = re.sub(r"_S_AZUCAR", "", sku)
        sku = re.sub(r"_SIN_GAS_625_ML$", "", sku)
        sku = re.sub(r"_SIN_GAS_625ML$", "", sku)
        sku = re.sub(r"_CON_GAS_625_ML$", "", sku)
        sku = re.sub(r"_CON_GAS_625ML$", "", sku)
        sku = re.sub(r"_SIN_GAS_750_ML$", "", sku)
        sku = re.sub(r"_SIN_GAS_750ML$", "", sku)
        sku = re.sub(r"_CON_GAS", "", sku)
        sku = re.sub(r"_SIN_GAS", "", sku)
        
        # Remove standalone _500ML but KEEP _1L, _1_5L, _2_25L, _3L for differentiation
        sku = re.sub(r"_500_ML$", "", sku)
        sku = re.sub(r"_500ML$", "", sku)
        sku = re.sub(r"_625_ML$", "", sku)
        sku = re.sub(r"_625ML$", "", sku)
        
        # Preserve sugar-free info when standalone (not with volume)
        sku = re.sub(r"_SIN_AZUCAR", "_SIN_AZUCAR", sku)  # Preserve SIN_AZUCAR
        
        return sku.strip("_")

    def build_comparable_sku(self, row: pd.Series) -> str:
        """Build cross-brand comparable SKU from sku_master or product attributes."""
        sku_master = row.get('sku_master', '')
        
        # Try to derive from sku_master first (cleaner extraction)
        if sku_master:
            cleaned = self._clean_sku_for_comparable(sku_master)
            if cleaned:
                return f"CMP_{cleaned}"
        
        # Fallback to attribute-based generation
        familia = self._normalize_sku_part(row.get('familia_producto'))
        subfamilia = self._normalize_sku_part(row.get('subfamilia'))
        tamano = self._normalize_sku_part(row.get('tamano'))
        tamano = re.sub(r"_SIN_TAMANO", "", tamano)  # Remove placeholder
        unidad = self._normalize_sku_part(row.get('unidad_base'))

        parts = ["CMP"]
        if familia:
            parts.append(familia)
        if subfamilia:
            parts.append(subfamilia)
        if tamano and tamano.strip():
            parts.append(tamano)
        if unidad:
            parts.append(unidad)

        if len(parts) == 1:
            fallback = self._normalize_sku_part(row.get('item_canonico') or row.get('item_fuente') or "ITEM")
            parts.append(fallback or "ITEM")

        return "_".join(parts)
    
    def save_local(self, suffix: str = "") -> Dict[str, str]:
        """
        Save dataframes to local CSV files
        
        Args:
            suffix: Optional suffix for file names
            
        Returns:
            Dictionary with file paths
        """
        files = {}
        
        if not self.df_maestro.empty:
            path_maestro = self.output_dir / f"maestro_{self.marca.lower()}{suffix}.csv"
            self.df_maestro.to_csv(path_maestro, index=False, encoding="utf-8")
            files['maestro'] = str(path_maestro)
            self.logger.info(f"✓ Maestro saved: {path_maestro.name} ({len(self.df_maestro)} rows)")
        
        if not self.df_precios.empty:
            path_precios = self.output_dir / f"precios_{self.marca.lower()}{suffix}.csv"
            self.df_precios.to_csv(path_precios, index=False, encoding="utf-8")
            files['precios'] = str(path_precios)
            self.logger.info(f"✓ Precios saved: {path_precios.name} ({len(self.df_precios)} rows)")
        
        if not self.df_raw.empty:
            path_raw = self.output_dir / f"datos_raw_{self.marca.lower()}{suffix}.csv"
            self.df_raw.to_csv(path_raw, index=False, encoding="utf-8")
            files['raw'] = str(path_raw)
            self.logger.info(f"✓ Raw data saved: {path_raw.name} ({len(self.df_raw)} rows)")
        
        return files
    
    def execute(self, save_local: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Execute complete scraping workflow
        
        Args:
            save_local: Whether to save files locally
            
        Returns:
            Dictionary with dataframes
        """
        self.logger.info(f"Starting scrape for {self.marca}...")
        
        try:
            self.df_raw = self.scrape()
            if self.df_raw.empty:
                self.logger.warning(f"No data retrieved for {self.marca}")
                return {'raw': self.df_raw, 'maestro': pd.DataFrame(), 'precios': pd.DataFrame()}
            
            self.logger.info(f"Raw data: {len(self.df_raw)} rows")
            self.process_data()
            
            if save_local:
                self.save_local()
            
            self.logger.info(f"✓ Scrape completed for {self.marca}")
            
            return {
                'raw': self.df_raw,
                'maestro': self.df_maestro,
                'precios': self.df_precios
            }
        
        except Exception as e:
            self.logger.error(f"Error during scraping: {str(e)}", exc_info=True)
            return {'raw': pd.DataFrame(), 'maestro': pd.DataFrame(), 'precios': pd.DataFrame()}
