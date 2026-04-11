import re

import requests

import pandas as pd

from bs4 import BeautifulSoup

 

BASE = "https://www.bembos.com.pe"

 

CATEGORIAS = {

    "Hamburguesas": f"{BASE}/menu/hamburguesas",

    "Complementos": f"{BASE}/menu/complementos",

    "Pollo": f"{BASE}/menu/pollo",

    "Menu y Ensaladas": f"{BASE}/menu/bembos-menus",

}

 

HEADERS = {

    "User-Agent": (

        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "

        "AppleWebKit/537.36 (KHTML, like Gecko) "

        "Chrome/123.0 Safari/537.36"

    )

}

 

def clean_text(text: str) -> str:

    return re.sub(r"\s+", " ", text or "").strip()

 

def parse_price(text: str):

    if not text:

        return None

    m = re.search(r"S/\s*([0-9]+(?:\.[0-9]{2})?)", text)

    return float(m.group(1)) if m else None

 

def infer_unidad_base(categoria: str, nombre: str):

    n = nombre.lower()

    if categoria == "Hamburguesas":

        return "unidad"

    if "papas" in n:

        return "porcion"

    if "ml" in n or "coca cola" in n or "inca kola" in n or "agua" in n:

        return "botella"

    return "unidad"

 

def infer_familia_producto(categoria: str, nombre: str):

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

 

def infer_subfamilia(categoria: str, nombre: str):

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

 

def build_item_canonico(nombre: str):

    nombre = clean_text(nombre)

    nombre = re.sub(r"^Hamburguesa\s+", "", nombre, flags=re.I)

    return nombre

 

def build_sku(marca: str, item_canonico: str, tamano):

    base = f"{marca}_{item_canonico}_{tamano or 'SIN_TAMANO'}".upper()

    return re.sub(r"[^A-Z0-9]+", "_", base).strip("_")

 

def aplica_tamanos(categoria: str, nombre: str) -> bool:

    n = nombre.lower()

 

    if categoria != "Complementos":

        return True

 

    if "papas" in n or "nuggets" in n:

        return True

 

    return False

 

def expandir_por_tamanos(

    marca: str,

    item_fuente: str,

    item_canonico_base: str,

    categoria_fuente: str,

    familia_producto: str,

    subfamilia: str,

    unidad_base: str,

    precio_medio: float,

    url_fuente: str,

):

    filas = []

 

    if aplica_tamanos(categoria_fuente, item_fuente):

        tamanos = [

            ("Regular", round(precio_medio - 3, 2)),

            ("Mediana", round(precio_medio, 2)),

            ("Grande", round(precio_medio + 3, 2)),

        ]

    else:

        tamanos = [(None, round(precio_medio, 2))]

 

    for tamano, precio in tamanos:

        item_canonico = item_canonico_base if tamano is None else f"{item_canonico_base} {tamano}"

        sku_master = build_sku(marca, item_canonico_base, tamano)

 

        filas.append({

            "marca": marca,

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

 

def scrape_bembos_category(url: str, categoria_fuente: str) -> list[dict]:

    r = requests.get(url, headers=HEADERS, timeout=30)

    r.raise_for_status()

 

    soup = BeautifulSoup(r.text, "html.parser")

    lines = [clean_text(x) for x in soup.get_text("\n").split("\n")]

    lines = [x for x in lines if x]

 

    results = []

    i = 0

    while i < len(lines):

        line = lines[i]

 

        if line == "Favoritos" and i >= 1:

            nombre = lines[i - 1]

 

            if nombre.lower() in {

                "favoritos", "mostrar", "productos", "ver todo",

                "más relevantes", "menos relevantes", "a a z", "z to a"

            }:

                i += 1

                continue

 

            descripcion = lines[i + 1] if i + 1 < len(lines) else ""

            precio_txt = lines[i + 2] if i + 2 < len(lines) else ""

            precio_medio = parse_price(precio_txt)

 

            if precio_medio is not None:

                item_canonico_base = build_item_canonico(nombre)

                familia_producto = infer_familia_producto(categoria_fuente, nombre)

                subfamilia = infer_subfamilia(categoria_fuente, nombre)

                unidad_base = infer_unidad_base(categoria_fuente, nombre)

 

                filas_expandida = expandir_por_tamanos(

                    marca="Bembos",

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

 

def scrape_bembos_menu():

    rows = []

    for categoria, url in CATEGORIAS.items():

        try:

            data = scrape_bembos_category(url, categoria)

            rows.extend(data)

            print(f"[OK] {categoria}: {len(data)} filas")

        except Exception as e:

            print(f"[WARN] No se pudo procesar {categoria}: {e}")

 

    df = pd.DataFrame(rows).drop_duplicates(

        subset=["sku_master", "precio_regular", "url_fuente"]

    )

 

    return df

 

if __name__ == "__main__":
    import os
    
    # Crear directorio output si no existe
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    df_raw = scrape_bembos_menu()

 

    df_maestro = (

        df_raw[

            [

                "marca",

                "item_fuente",

                "item_canonico",

                "sku_master",

                "familia_producto",

                "subfamilia",

                "tamano",

                "unidad_base",

            ]

        ]

        .drop_duplicates(subset=["sku_master"])

        .reset_index(drop=True)

    )

 

    fecha_captura = pd.Timestamp.today().normalize()

 

    df_precios = (

        df_raw[

            [

                "sku_master",

                "precio_regular",

                "precio_base_fuente",

                "url_fuente",

            ]

        ]

        .drop_duplicates(subset=["sku_master", "precio_regular", "url_fuente"])

        .assign(

            fecha_inicio=fecha_captura,

            fecha_fin=pd.NaT,

            fuente_precio="web_bembos"

        )[

            [

                "sku_master",

                "fecha_inicio",

                "fecha_fin",

                "precio_regular",

                "fuente_precio",

                "url_fuente",

                "precio_base_fuente",

            ]

        ]

        .reset_index(drop=True)

    )
    
    # Guardar DataFrames en CSV
    df_maestro.to_csv(os.path.join(output_dir, "maestro_productos.csv"), index=False, encoding="utf-8")
    df_precios.to_csv(os.path.join(output_dir, "precios_productos.csv"), index=False, encoding="utf-8")
    df_raw.to_csv(os.path.join(output_dir, "datos_raw.csv"), index=False, encoding="utf-8")
    
    print("\n✓ Datos guardados en archivos CSV:")
    print(f"  - {output_dir}/maestro_productos.csv ({len(df_maestro)} filas)")
    print(f"  - {output_dir}/precios_productos.csv ({len(df_precios)} filas)")
    print(f"  - {output_dir}/datos_raw.csv ({len(df_raw)} filas)")

 

    pd.set_option("display.max_rows", None)

    pd.set_option("display.max_columns", None)

    pd.set_option("display.width", 200)

    pd.set_option("display.max_colwidth", 120)

 

    print("\n================ DF RAW ================\n")

    print(df_raw.to_csv(index=False).strip())

 

    # print("\n================ DF MAESTRO ================\n")

    # print(df_maestro.to_csv(index=False).strip())

 

    # print("\n================ DF PRECIOS ================\n")

    # print(df_precios.to_csv(index=False).strip())