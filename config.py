"""
Configuration and constants
"""

from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).parent

# Directories
OUTPUT_DIR = PROJECT_ROOT / "output"
SCRAPERS_DIR = PROJECT_ROOT / "scrapers"

# Output files
CONSOLIDATED_MAESTRO = OUTPUT_DIR / "consolidated_maestro.csv"
CONSOLIDATED_PRECIOS = OUTPUT_DIR / "consolidated_precios.csv"
CONSOLIDATED_RAW = OUTPUT_DIR / "consolidated_raw.csv"

# Logging
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_LEVEL = 'INFO'

# List of active scrapers (add new scrapers here)
ACTIVE_SCRAPERS = [
    'BembosScraper',
    'PopeyesScraper',
    'KFCScraper',
    'PizzaHutScraper',
    # 'SubwayScraper',     # Example: uncomment when added
]
