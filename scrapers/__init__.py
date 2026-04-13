"""
Scrapers package - Contains all web scrapers for restaurant menus
"""

from .bembos_scraper import BembosScraper
from .popeyes_scraper import PopeyesScraper
from .kfc_scraper import KFCScraper
from .pizzahut_scraper import PizzaHutScraper
from .rokys_scraper import RokysScraper
from .chinawok_scraper import ChinawokScraper

__all__ = [
    'BembosScraper',
    'PopeyesScraper',
    'KFCScraper',
    'PizzaHutScraper',
    'RokysScraper',
    'ChinawokScraper',
]
