"""
Main orchestrator
Executes all scrapers and consolidates data
"""

import sys
import logging
from pathlib import Path
import pandas as pd
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    OUTPUT_DIR, CONSOLIDATED_MAESTRO, CONSOLIDATED_PRECIOS, 
    CONSOLIDATED_RAW, ACTIVE_SCRAPERS, LOG_FORMAT, LOG_LEVEL
)
from scrapers import BembosScraper, PopeyesScraper, KFCScraper, PizzaHutScraper, RokysScraper, ChinawokScraper, DunkinScraper

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=LOG_LEVEL,
        format=LOG_FORMAT
    )
    return logging.getLogger(__name__)


def get_scraper_instance(scraper_name):
    """
    Get scraper instance by name
    
    Args:
        scraper_name: Name of the scraper class
        
    Returns:
        Scraper instance or None
    """
    scrapers_map = {
        'BembosScraper': BembosScraper,
        'PopeyesScraper': PopeyesScraper,
        'KFCScraper': KFCScraper,
        'PizzaHutScraper': PizzaHutScraper,
        'RokysScraper': RokysScraper,
        'ChinawokScraper': ChinawokScraper,
        'DunkinScraper': DunkinScraper
        # Add more scrapers here as you create them
        # 'McdonaldsScraper': McdonaldsScraper,
    }
    
    scraper_class = scrapers_map.get(scraper_name)
    if scraper_class:
        return scraper_class(output_dir=str(OUTPUT_DIR))
    return None


def consolidate_data(all_maestro, all_precios, all_raw, logger):
    """
    Consolidate data from all scrapers
    
    Args:
        all_maestro: List of maestro dataframes
        all_precios: List of precios dataframes
        all_raw: List of raw dataframes
        logger: Logger instance
    """
    logger.info("\n" + "="*60)
    logger.info("CONSOLIDATING DATA")
    logger.info("="*60)
    
    # Consolidate maestro
    if all_maestro:
        df_maestro_consolidated = pd.concat(all_maestro, ignore_index=True).drop_duplicates(
            subset=['sku_master']
        ).reset_index(drop=True)
        df_maestro_consolidated.to_csv(CONSOLIDATED_MAESTRO, index=False, encoding="utf-8")
        logger.info(f"✓ Consolidated maestro: {CONSOLIDATED_MAESTRO.name} ({len(df_maestro_consolidated)} rows)")
    
    # Consolidate precios
    if all_precios:
        df_precios_consolidated = pd.concat(all_precios, ignore_index=True).drop_duplicates(
            subset=['sku_master', 'precio_regular', 'fuente_precio']
        ).reset_index(drop=True)
        df_precios_consolidated.to_csv(CONSOLIDATED_PRECIOS, index=False, encoding="utf-8")
        logger.info(f"✓ Consolidated precios: {CONSOLIDATED_PRECIOS.name} ({len(df_precios_consolidated)} rows)")
    
    # Consolidate raw
    if all_raw:
        df_raw_consolidated = pd.concat(all_raw, ignore_index=True).reset_index(drop=True)
        df_raw_consolidated.to_csv(CONSOLIDATED_RAW, index=False, encoding="utf-8")
        logger.info(f"✓ Consolidated raw data: {CONSOLIDATED_RAW.name} ({len(df_raw_consolidated)} rows)")


def main():
    """Main execution function"""
    logger = setup_logging()
    
    logger.info("\n" + "="*60)
    logger.info("BENEFITS WEB SCRAPING ORCHESTRATOR")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    all_maestro = []
    all_precios = []
    all_raw = []
    executed_scrapers = []
    failed_scrapers = []
    
    # Execute each active scraper
    logger.info(f"\nExecuting {len(ACTIVE_SCRAPERS)} scrapers...")
    logger.info("-"*60)
    
    for scraper_name in ACTIVE_SCRAPERS:
        try:
            scraper = get_scraper_instance(scraper_name)
            if not scraper:
                logger.warning(f"✗ Scraper not found: {scraper_name}")
                failed_scrapers.append(scraper_name)
                continue
            
            result = scraper.execute(save_local=True)
            
            if not result['raw'].empty:
                all_maestro.append(result['maestro'])
                all_precios.append(result['precios'])
                all_raw.append(result['raw'])
                executed_scrapers.append(scraper_name)
            else:
                failed_scrapers.append(scraper_name)
            
            logger.info("")
        
        except Exception as e:
            logger.error(f"✗ Error executing {scraper_name}: {str(e)}")
            failed_scrapers.append(scraper_name)
    
    # Consolidate all data
    logger.info("-"*60)
    consolidate_data(all_maestro, all_precios, all_raw, logger)
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("EXECUTION SUMMARY")
    logger.info("="*60)
    logger.info(f"✓ Successful scrapers: {len(executed_scrapers)}")
    for scraper in executed_scrapers:
        logger.info(f"  - {scraper}")
    
    if failed_scrapers:
        logger.warning(f"✗ Failed scrapers: {len(failed_scrapers)}")
        for scraper in failed_scrapers:
            logger.warning(f"  - {scraper}")
    
    logger.info(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info("="*60 + "\n")


if __name__ == "__main__":
    main()
