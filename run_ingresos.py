"""
run_ingresos.py
===============
Script ejecutable independiente para la migración de ingresos.
Puede ejecutarse directamente:
    python run_ingresos.py

O importarse desde main.py:
    import run_ingresos; run_ingresos.run(dfs_limpios)
"""

import os
import pandas as pd

from dotenv import load_dotenv

from ingresos_migration import build_ingresos_df
from ingresos_migration.exporter_sql import export_ingresos_to_sql
from excel_export import export_book_to_excel

load_dotenv()


def run(dfs_limpios: dict):
    """
    Corre la migración de ingresos.

    Args:
        dfs_limpios: dict {nombre_hoja: DataFrame} ya limpios (de run_catalogo_items)
    """
    print("\n" + "=" * 60)
    print("MIGRACIÓN: ingresos")
    print("=" * 60)

    df_ingresos = build_ingresos_df(dfs_limpios)

    print(
        f"\nDataFrame de ingresos listo: "
        f"{df_ingresos.shape[0]} filas x {df_ingresos.shape[1]} columnas"
    )
    print(df_ingresos.head(3).to_string())

    ruta_sql = export_ingresos_to_sql(df_ingresos, filename="ingresos.sql")
    print(f"\n[run_ingresos] SQL generado: {ruta_sql}")
    return df_ingresos


if __name__ == "__main__":
    from run_catalogo_items import load_dfs_limpios
    
    dfs = load_dfs_limpios()
    run(dfs)
