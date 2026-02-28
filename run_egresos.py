"""
run_egresos.py
==============
Script pequeño y directo para la migración de egresos.

PRERREQUISITO:
  - Haber ejecutado antes los SQL de `ingresos` e `ingreso_detalles`
    para que existan registros en la tabla `ingreso_detalles` con id > 7.

Uso desde código / main.py:
    import run_egresos
    run_egresos.run(dfs_limpios, engine)
"""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

from egresos_migration import build_egresos_df, export_egresos_to_sql
from utils.logger import get_logger

load_dotenv()

# Logger compartido (archivo + consola) para mostrar el avance del proceso
logger = get_logger()


def _get_engine():
    """
    Crea un engine de SQLAlchemy usando las variables de entorno
    de la conexión MySQL (DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME).
    """
    url = (
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '3306')}"
        f"/{os.getenv('DB_NAME')}"
    )
    return create_engine(url)


def run(dfs_limpios: dict, engine=None):
    """
    Ejecuta la migración de egresos de principio a fin.

    Args:
        dfs_limpios:
            Diccionario {nombre_hoja: DataFrame} con las hojas del Excel ya
            limpiadas por `run_catalogo_items.load_dfs_limpios`.
        engine:
            Conexión SQLAlchemy ya creada. Si es None, se crea internamente.
    """
    logger.info("=" * 60)
    logger.info("MIGRACIÓN: egresos")
    logger.info("=" * 60)

    # Si no nos pasan un engine, lo creamos aquí
    if engine is None:
        engine = _get_engine()

    # 1) Construir el DataFrame final de egresos (solo memoria)
    df_egresos = build_egresos_df(dfs_limpios, engine)

    logger.info(
        f"DataFrame egresos listo: {df_egresos.shape[0]} filas x {df_egresos.shape[1]} columnas"
    )

    # 2) Exportar ese DataFrame a un archivo SQL listo para ejecutar en MySQL
    ruta_sql = export_egresos_to_sql(df_egresos, filename="egresos.sql")
    logger.info(f"[run_egresos] SQL generado: {ruta_sql}")

    return df_egresos


if __name__ == "__main__":
    # Si se ejecuta este archivo directamente:
    # 1) Cargamos y limpiamos el Excel una sola vez
    from run_catalogo_items import load_dfs_limpios

    dfs = load_dfs_limpios()
    # 2) Ejecutamos la migración de egresos usando esa información
    run(dfs)
