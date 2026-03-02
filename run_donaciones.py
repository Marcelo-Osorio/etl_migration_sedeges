"""
run_donaciones.py
=================
Script ejecutable independiente para la migración de DONACIONES.

Genera un único archivo output/donaciones.sql que inserta en las tablas
ingresos, ingreso_detalles y egresos, usando @variables MySQL para
enlazar los IDs automáticamente.

PREREQUISITO:
  - La BD debe tener la tabla almacens con id=25 = 'DONACION SIN ALMACEN'
  - La BD debe tener la tabla users con id=1 = 'admin'
  - Las partidas referenciadas en la hoja DONACIONES deben existir en
    la tabla partidas

Puede ejecutarse directamente:
    python run_donaciones.py

O importarse desde main.py:
    import run_donaciones; run_donaciones.run(dfs_limpios, engine)
"""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

from donaciones_migration import build_donaciones_sql
from utils.logger import get_logger

load_dotenv()

logger = get_logger()


def _get_engine():
    url = (
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '3306')}"
        f"/{os.getenv('DB_NAME')}"
    )
    return create_engine(url)


def run(dfs_limpios: dict, engine=None):
    """
    Ejecuta la migración de DONACIONES de principio a fin.

    Args:
        dfs_limpios: dict {nombre_hoja: DataFrame} ya limpiados
        engine:      SQLAlchemy engine (opcional; si None lo crea internamente)
    """
    logger.info("=" * 60)
    logger.info("MIGRACIÓN: DONACIONES")
    logger.info("=" * 60)

    if engine is None:
        engine = _get_engine()

    ruta_sql = build_donaciones_sql(dfs_limpios, engine)
    logger.info(f"[run_donaciones] SQL generado: {ruta_sql}")

    return ruta_sql


if __name__ == "__main__":
    from excel_loader import load_dfs_limpios

    dfs = load_dfs_limpios()
    run(dfs)
