"""
items.migration
===============
Módulo encargado de extraer, transformar y exportar el catálogo de ítems
desde las hojas *detalle* del Excel hacia la tabla `catalogo_items` de la DB.

Uso principal:
    from items.migration import build_catalogo_items_df
    df = build_catalogo_items_df(dfs_limpios)
"""

from items_migration.extractor import extract_items_from_detalle
from items_migration.transformer import transform_items
from items_migration.exporter_sql import export_items_to_sql, export_items_to_db


def build_catalogo_items_df(dfs_limpios: dict):
    """
    Pipeline completo:
      1. Extrae ítems de las hojas detalle
      2. Aplica transformaciones (nombre, grupo, abreviatura)

    Args:
        dfs_limpios: dict de {nombre_hoja: DataFrame} ya limpiados por rules.py

    Returns:
        DataFrame listo para exportar a catalogo_items
    """
    # Paso 1: extraer campos relevantes de todas las hojas detalle
    df_raw = extract_items_from_detalle(dfs_limpios)

    # Paso 2: transformar y generar columnas finales
    df_final = transform_items(df_raw)

    return df_final


__all__ = [
    "build_catalogo_items_df",
    "extract_items_from_detalle",
    "transform_items",
    "export_items_to_sql",
    "export_items_to_db",
]
