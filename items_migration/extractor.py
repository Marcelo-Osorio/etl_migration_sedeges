"""
items.migration.extractor
=========================
Extrae los campos necesarios de TODAS las hojas detalle del libro Excel ya
limpiado. Las hojas detalle son las definidas bajo la clave "detalles" en
utils/tables.excel.organization.json, es decir:

    ANEXO-1A, PCVH MUJER, PAIPI, CEPAT, PDP MUJERES IDH,
    PAAMCTI PROVINCIAL, PPAER PENAL, POAR PENAL, PMA NIÑNIÑO ADOL,
    PCEMTRATA Y TRAFICO, BECAS, REFRIGERIOS, VIVERES FRESCOS,
    FONDOS EN AVANCE, FARMACIA

De cada hoja extrae: DESCRIPCION, CODIGO, UNIDAD, y (solo FARMACIA) GRUPO.
"""

import json
import os
import pandas as pd

# ------------------------------------------------------------------ #
# Carga la lista de hojas detalle desde el JSON de organización       #
# ------------------------------------------------------------------ #
_JSON_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "utils",
    "tables.excel.organization.json",
)

with open(_JSON_PATH, encoding="utf-8") as _f:
    _org = json.load(_f)

# Conjunto de nombres de hojas que son "detalle"
HOJAS_DETALLE: set[str] = set(_org["detalles"].keys())


def extract_items_from_detalle(dfs_limpios: dict) -> pd.DataFrame:
    """
    Recorre el diccionario de DataFrames ya limpios (producido por main.py)
    y extrae únicamente las hojas detalle.

    Por cada hoja recupera:
        - DESCRIPCION  → nombre del ítem
        - CODIGO       → código de partida (ej. "ALM-10", "P-3")
        - UNIDAD       → unidad de medida en texto (ej. "PIEZA", "SERVICIO")
        - GRUPO        → solo disponible en FARMACIA (ya está en el df)

    SE Agrego la columna `hoja_origen` para trazabilidad.

    Args:
        dfs_limpios: dict {nombre_hoja: DataFrame} producido por procesar_etl()

    Returns:
        DataFrame consolidado con columnas:
            hoja_origen, DESCRIPCION, CODIGO, UNIDAD, GRUPO (NaN si no aplica)
    """
    fragmentos = []

    for nombre_hoja, df in dfs_limpios.items():
        # Solo procesamos hojas que están en el conjunto de detalle
        if nombre_hoja not in HOJAS_DETALLE:
            continue

        # --- Extraer columnas comunes ---
        columnas_requeridas = ["DESCRIPCION", "CODIGO", "UNIDAD"]

        # Verificar que las columnas existen en el df
        cols_presentes = [c for c in columnas_requeridas if c in df.columns]
        fragmento = df[cols_presentes].copy()

        # --- FARMACIA: ya tiene columna GRUPO generada en clean_farmacia() ---
        if nombre_hoja == "FARMACIA" and "GRUPO" in df.columns:
            fragmento["GRUPO"] = df["GRUPO"].values
        else:
            # Para el resto de hojas no existe GRUPO propio
            fragmento["GRUPO"] = None

        # Marca de qué hoja proviene (útil para debug)
        fragmento["hoja_origen"] = nombre_hoja

        fragmentos.append(fragmento)

    if not fragmentos:
        # Si no encontró ninguna hoja detalle retorna df vacío
        return pd.DataFrame(
            columns=["hoja_origen", "DESCRIPCION", "CODIGO", "UNIDAD", "GRUPO"]
        )

    # Unir todos los fragmentos en un solo DataFrame
    df_total = pd.concat(fragmentos, ignore_index=True)

    # Eliminar filas donde DESCRIPCION sea nula o vacía
    df_total = df_total[
        df_total["DESCRIPCION"].notna()
        & (df_total["DESCRIPCION"].astype(str).str.strip() != "")
    ].copy()

    df_total.reset_index(drop=True, inplace=True)

    print(f"[extractor] Total ítems extraídos de hojas detalle: {len(df_total)}")

    return df_total
