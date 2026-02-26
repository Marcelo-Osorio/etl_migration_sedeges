"""
run_ingreso_detalles.py
=======================
Script ejecutable independiente para la migración de ingreso_detalles.

PREREQUISITO: ingresos.sql debe haber sido ejecutado en la DB primero.

Acciones:
  1. Renombra producto_id → item_id en ingreso_detalles (si aún no se hizo)
  2. Construye el DataFrame de ingreso_detalles (con lookups a DB + fallback auto-insert)
  3. Genera output/ingreso_detalles.sql (INSERTs + UPDATEs de total y etapa_ingreso)

Puede ejecutarse directamente:
    python run_ingreso_detalles.py

O importarse desde main.py:
    import run_ingreso_detalles; run_ingreso_detalles.run(dfs_limpios, engine)
"""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from ingreso_detalles_migration import build_ingreso_detalles_df
from ingreso_detalles_migration.exporter_sql import export_ingreso_detalles_to_sql

load_dotenv()


def _get_engine():
    url = (
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '3306')}"
        f"/{os.getenv('DB_NAME')}"
    )
    return create_engine(url)



def run(dfs_limpios: dict, engine=None):
    """
    Corre la migración de ingreso_detalles.

    Args:
        dfs_limpios: dict {nombre_hoja: DataFrame} ya limpios
        engine:      SQLAlchemy engine (opcional; si None lo crea internamente)
    """
    print("\n" + "=" * 60)
    print("MIGRACIÓN: ingreso_detalles")
    print("=" * 60)

    if engine is None:
        engine = _get_engine()
    # Paso 1: Construir DataFrame
    df_detalles, etapas_df = build_ingreso_detalles_df(dfs_limpios, engine)

    print(
        f"\nDataFrame de ingreso_detalles listo: "
        f"{df_detalles.shape[0]} filas x {df_detalles.shape[1]} columnas"
    )
    print(df_detalles.head(3).to_string())

    # Paso 2: Exportar SQL
    ruta_sql = export_ingreso_detalles_to_sql(
        df_detalles, etapas_df, filename="ingreso_detalles.sql"
    )
    print(f"\n[run_ingreso_detalles] SQL generado: {ruta_sql}")
    return df_detalles


if __name__ == "__main__":
    from run_catalogo_items import load_dfs_limpios

    dfs = load_dfs_limpios()
    run(dfs)
