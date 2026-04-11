"""
Pizza Hut scraper implementation
Scrapes Pizza Hut Peru menu for pizzas, antojitos and beverages.
"""

import re
import unicodedata
from typing import Dict, List
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright

from .base_scraper import BaseScraper


class PizzaHutScraper(BaseScraper):
    """Scraper for Pizza Hut Peru menu."""

    BASE_URL = "https://www.pizzahut.com.pe"
    PIZZAS_URL = f"{BASE_URL}/carta/pizzas"
    ANTOJITOS_URL = f"{BASE_URL}/carta/antojitos?page={{page}}"
    BEBIDAS_URL = f"{BASE_URL}/carta/bebidas"

    CLASSIC_PIZZAS = {
        "pizza-americana": "AMERICANA",
        "pizza-hawaiana": "HAWAIANA",
        "pizza-mozzarella": "MOZZARELLA",
        "pizza-pepperoni": "PEPPERONI",
    }

    SPECIAL_PIZZAS = {
        "pizza-chicken-bbq": "CHICKEN_BBQ",
        "pizza-chili-hut": "CHILI_HUT",
        "pizza-continental": "CONTINENTAL",
        "pizza-meat-lovers": "MEAT_LOVERS",
        "pizza-suprema": "SUPREMA",
        "pizza-super-suprema": "SUPER_SUPREMA",
        "pizza-vegetariana": "VEGETARIANA",
    }

    PIZZA_SIZE_RULES = {
        "classic": ["Personal", "Mediana", "Grande", "Familiar"],
        "special": ["Mediana", "Grande", "Familiar"],
        "xl": ["XL"],
    }

    ANTOJITOS_COUNTS = {
        "palitos-a-la-siciliana": 6,
        "pan-al-ajo-4-un": 4,
        "pan-al-ajo-especial-4-un": 4,
        "rolls-de-jamon-queso": 6,
        "alitas": 6,
        "hut-bread-8-un": 8,
        "rolls-de-manjar": 6,
        "hut-churros": 4,
        "volcan-de-limon": 1,
        "volcan-de-chocolate": 1,
        "salsa-chili-thai": 1,
        "salsa-honey-bbq": 1,
        "salsa-mayohut": 1,
        "salsa-mediterranea": 1,
    }

    BEBIDAS_RULES = {
        "agua-san-luis-s-gas-personal": {"item": "AGUA_SAN_LUIS", "size": "Personal"},
        "coca-cola-sin-azucar": {"item": "COCA_COLA_SIN_AZUCAR", "size": "1L"},
        "sprite-personal": {"item": "SPRITE", "size": "Personal"},
        "fanta-personal": {"item": "FANTA", "size": "Personal"},
        "inca-kola-sin-azucar": {"item": "INCA_KOLA_SIN_AZUCAR", "size": "1L"},
    }

    def __init__(self, output_dir: str = "output"):
        super().__init__(output_dir=output_dir, marca="PizzaHut")

    @staticmethod
    def clean_text(text: str) -> str:
        return re.sub(r"\s+", " ", text or "").strip()

    @staticmethod
    def parse_price(text: str) -> float | None:
        if not text:
            return None
        matches = re.findall(r"S/\s*(\d+(?:\.\d{2})?)", text)
        if matches:
            return float(matches[-1])
        return None

    @staticmethod
    def _normalize_accents(text: str) -> str:
        if text is None:
            return ""
        text = str(text).strip().upper()
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        text = re.sub(r"[^A-Z0-9]+", "_", text)
        return text.strip("_")

    def build_sku(self, marca: str, item_name: str, tamano: str = None) -> str:
        base = f"{marca}_{item_name}"
        if tamano:
            base = f"{base}_{tamano}"
        base = self._normalize_accents(base)
        return base

    @staticmethod
    def _resolve_chrome_executable() -> str | None:
        playwright_root = Path.home() / "AppData" / "Local" / "ms-playwright"
        for candidate in sorted(playwright_root.glob("chromium-*/chrome-win64/chrome.exe"), reverse=True):
            if candidate.exists():
                return str(candidate)
        return None

    def _fetch_cards(self, page, url: str, prefix: str) -> List[Dict[str, str]]:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2000)

        cards = page.eval_on_selector_all(
            f'a[href^="{prefix}"]',
            """
            elements => elements.map(element => ({
                href: element.getAttribute('href'),
                text: (element.innerText || element.textContent || '').trim(),
            }))
            """,
        )

        seen = set()
        cleaned = []
        for card in cards:
            href = card.get("href") or ""
            text = self.clean_text(card.get("text") or "")
            if not href or not text or "S/" not in text:
                continue
            if href in seen:
                continue
            seen.add(href)
            cleaned.append({"href": href, "text": text})
        return cleaned

    def _build_pizza_rows(self, card: Dict[str, str]) -> List[Dict[str, object]]:
        href = card["href"]
        text = card["text"]
        slug = href.strip("/").split("/")[-1]
        price = self.parse_price(text)
        title = text.split("\n")[0].strip()

        rows = []

        if slug == "pizza-xl-clasica":
            item_key = "PIZZA_XL_CLASICA"
            sku_master = self.build_sku(self.marca, item_key)
            rows.append(
                {
                    "marca": self.marca,
                    "item_fuente": title,
                    "item_canonico": "Pizza XL Clasica",
                    "sku_master": sku_master,
                    "familia_producto": "Pizza",
                    "subfamilia": "Clasica",
                    "tamano": "XL",
                    "unidad_base": "unidad",
                    "precio_regular": price,
                    "categoria_fuente": "Pizzas",
                    "url_fuente": f"{self.BASE_URL}{href}",
                    "precio_base_fuente": price,
                }
            )
            return rows

        if slug in self.CLASSIC_PIZZAS:
            item_base = self.CLASSIC_PIZZAS[slug]
            for size in self.PIZZA_SIZE_RULES["classic"]:
                sku_master = self.build_sku(self.marca, f"{item_base}_CLASICA", size)
                rows.append(
                    {
                        "marca": self.marca,
                        "item_fuente": title,
                        "item_canonico": f"{item_base.title().replace('_', ' ')} Clasica {size}",
                        "sku_master": sku_master,
                        "familia_producto": "Pizza",
                        "subfamilia": "Clasica",
                        "tamano": size,
                        "unidad_base": "unidad",
                        "precio_regular": price,
                        "categoria_fuente": "Pizzas",
                        "url_fuente": f"{self.BASE_URL}{href}",
                        "precio_base_fuente": price,
                    }
                )
            return rows

        if slug in self.SPECIAL_PIZZAS:
            item_base = self.SPECIAL_PIZZAS[slug]
            for size in self.PIZZA_SIZE_RULES["special"]:
                sku_master = self.build_sku(self.marca, f"{item_base}_ESPECIAL", size)
                rows.append(
                    {
                        "marca": self.marca,
                        "item_fuente": title,
                        "item_canonico": f"{item_base.title().replace('_', ' ')} Especial {size}",
                        "sku_master": sku_master,
                        "familia_producto": "Pizza",
                        "subfamilia": "Especial",
                        "tamano": size,
                        "unidad_base": "unidad",
                        "precio_regular": price,
                        "categoria_fuente": "Pizzas",
                        "url_fuente": f"{self.BASE_URL}{href}",
                        "precio_base_fuente": price,
                    }
                )
            return rows

        return rows

    def _build_antojito_row(self, card: Dict[str, str]) -> Dict[str, object]:
        href = card["href"]
        text = card["text"]
        slug = href.strip("/").split("/")[-1]
        title = text.split("\n")[0].strip()
        price = self.parse_price(text)
        count = self.ANTOJITOS_COUNTS.get(slug, 1)

        canonical = self._normalize_accents(title)
        canonical = canonical.replace("&", "Y")
        canonical = re.sub(r"\([^\)]*\)", "", canonical)
        canonical = re.sub(r"\s+", " ", canonical).strip()

        sku_item = self._normalize_accents(title)
        sku_item = sku_item.replace("&", "Y")
        sku_item = re.sub(r"\([^\)]*\)", "", sku_item)
        sku_item = sku_item.replace(" ", "_")
        sku_item = re.sub(r"[^A-Z0-9_]+", "", sku_item)
        sku_item = sku_item.strip("_")

        sku_master = self.build_sku(self.marca, sku_item, f"{count}_UN" if count > 1 else None)

        return {
            "marca": self.marca,
            "item_fuente": title,
            "item_canonico": canonical,
            "sku_master": sku_master,
            "familia_producto": "Antojito",
            "subfamilia": None,
            "tamano": f"{count}_UN" if count > 1 else None,
            "unidad_base": "unidad",
            "precio_regular": round(price / count, 2) if count and price is not None else price,
            "categoria_fuente": "Antojitos",
            "url_fuente": f"{self.BASE_URL}{href}",
            "precio_base_fuente": price,
        }

    def _build_bebida_row(self, card: Dict[str, str]) -> Dict[str, object]:
        href = card["href"]
        text = card["text"]
        slug = href.strip("/").split("/")[-1]
        title = text.split("\n")[0].strip()
        price = self.parse_price(text)
        rule = self.BEBIDAS_RULES.get(slug)

        if rule:
            item = rule["item"]
            size = rule["size"]
        else:
            item = self._normalize_accents(title)
            item = item.replace(" ", "_")
            item = re.sub(r"[^A-Z0-9_]+", "", item)
            size = None

        sku_master = self.build_sku(self.marca, item, size)

        return {
            "marca": self.marca,
            "item_fuente": title,
            "item_canonico": self.clean_text(title),
            "sku_master": sku_master,
            "familia_producto": "Bebida",
            "subfamilia": None,
            "tamano": size,
            "unidad_base": "botella",
            "precio_regular": price,
            "categoria_fuente": "Bebidas",
            "url_fuente": f"{self.BASE_URL}{href}",
            "precio_base_fuente": price,
        }

    def scrape(self) -> pd.DataFrame:
        rows: List[Dict[str, object]] = []

        with sync_playwright() as playwright:
            chrome_executable = self._resolve_chrome_executable()
            if chrome_executable:
                browser = playwright.chromium.launch(headless=True, executable_path=chrome_executable)
            else:
                browser = playwright.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 1200})

            try:
                pizza_cards = self._fetch_cards(page, self.PIZZAS_URL, "/pizzas/")
                self.logger.info(f"✓ Pizzas: {len(pizza_cards)} cards")
                for card in pizza_cards:
                    rows.extend(self._build_pizza_rows(card))

                antojitos_cards = []
                for page_number in [1, 2]:
                    cards = self._fetch_cards(page, self.ANTOJITOS_URL.format(page=page_number), "/antojitos/")
                    self.logger.info(f"✓ Antojitos page {page_number}: {len(cards)} cards")
                    antojitos_cards.extend(cards)
                for card in antojitos_cards:
                    rows.append(self._build_antojito_row(card))

                bebidas_cards = self._fetch_cards(page, self.BEBIDAS_URL, "/bebidas/")
                self.logger.info(f"✓ Bebidas: {len(bebidas_cards)} cards")
                for card in bebidas_cards:
                    rows.append(self._build_bebida_row(card))
            finally:
                browser.close()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows).drop_duplicates(subset=["sku_master", "precio_regular", "url_fuente"])
        return df.reset_index(drop=True)
