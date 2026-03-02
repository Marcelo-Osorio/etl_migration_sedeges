"""
donaciones_migration
=====================
Módulo ETL para la migración de la hoja DONACIONES del Excel.

Genera un único archivo SQL (output/donaciones.sql) con INSERTs
encadenados para las tablas ingresos, ingreso_detalles y egresos,
usando variables MySQL para enlazar los IDs.

Uso:
    from donaciones_migration import build_donaciones_sql
    ruta = build_donaciones_sql(dfs_limpios, engine)
"""

from sqlalchemy.engine import Engine

from donaciones_migration.validator import run_all_validations
from donaciones_migration.transformer import build_donaciones_dfs
from donaciones_migration.exporter_sql import export_donaciones_to_sql
from utils.logger import get_logger

logger = get_logger()

__all__ = ["build_donaciones_sql"]


def build_donaciones_sql(dfs_limpios: dict, engine: Engine) -> str:
    """
    Orquesta todo el proceso de migración de DONACIONES.

    Pasos:
      1) Extraer el DataFrame DONACIONES de dfs_limpios
      2) Validar almacen_id=25, user_id=1, resolver partida_ids
      3) Construir los 3 DataFrames (ingresos, ingreso_detalles, egresos)
      4) Generar output/donaciones.sql

    Args:
        dfs_limpios: dict {nombre_hoja: DataFrame} ya limpiados
        engine:      SQLAlchemy engine conectado a la BD

    Returns:
        Ruta absoluta del archivo SQL generado

    Raises:
        RuntimeError: si alguna validación falla
        KeyError: si la hoja DONACIONES no existe en dfs_limpios
    """
    if "DONACIONES" not in dfs_limpios:
        msg = (
            "La hoja 'DONACIONES' no se encuentra en dfs_limpios. "
            "Hojas disponibles: " + str(list(dfs_limpios.keys()))
        )
        logger.error(msg)
        raise KeyError(msg)

    df_donaciones = dfs_limpios["DONACIONES"]
    logger.info(f"Hoja DONACIONES cargada: {len(df_donaciones)} filas")
    logger.info(f"Columnas: {list(df_donaciones.columns)}")

    partida_ids = run_all_validations(df_donaciones, engine)

    df_ing, df_det, df_egr = build_donaciones_dfs(df_donaciones, partida_ids)

    ruta = export_donaciones_to_sql(df_ing, df_det, df_egr)

    return ruta
