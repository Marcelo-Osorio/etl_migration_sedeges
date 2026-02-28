"""
egresos_migration
==================

Módulo ETL de alto nivel para la tabla `egresos`.

Aquí no hay lógica complicada: solo juntamos las piezas:
  - extractor: lee Excel + DB
  - transformer: construye el DataFrame con la forma de `egresos`
  - exporter_sql: genera el archivo SQL final
"""

from egresos_migration.extractor import (
    build_df_limpio_unificado,
    fetch_catalogo_items_nombres,
    fetch_ingreso_detalles_gt7,
)
from egresos_migration.exporter_sql import export_egresos_to_sql
from egresos_migration.transformer import build_egresos_df as _build_egresos_df_transformed

from utils.logger import get_logger

__all__ = [
    "build_egresos_df",
    "build_df_limpio_unificado",
    "fetch_ingreso_detalles_gt7",
    "fetch_catalogo_items_nombres",
    "export_egresos_to_sql",
]


def build_egresos_df(dfs_limpios: dict, engine):
    """
    Orquesta todo el proceso de construcción del DataFrame de egresos.

    Pasos:
      1) Unir todas las SALIDAS del Excel en un solo DataFrame (por almacén)
      2) Leer ingreso_detalles (id > 7) desde la DB
      3) Leer catalogo_items (id, nombre) desde la DB
      4) Llamar al transformer para aplicar la lógica y validaciones

    Devuelve:
      DataFrame listo para pasarlo a `export_egresos_to_sql`.
    """
    logger = get_logger()

    df_limpio = build_df_limpio_unificado(dfs_limpios)
    logger.info(f"DataFrame limpio unificado: {len(df_limpio)} filas")

    df_ingreso_detalles = fetch_ingreso_detalles_gt7(engine)
    logger.info(f"ingreso_detalles (id > 7): {len(df_ingreso_detalles)} filas")

    item_id_to_nombre = fetch_catalogo_items_nombres(engine)
    logger.info(f"catalogo_items cargados: {len(item_id_to_nombre)} ítems")

    return _build_egresos_df_transformed(
        df_ingreso_detalles, df_limpio, item_id_to_nombre
    )
