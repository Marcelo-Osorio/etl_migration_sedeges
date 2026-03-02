"""
run_catalogo_items.py
=====================
Script ejecutable independiente para la migración de catalogo_items.
Puede ejecutarse directamente:
    python run_catalogo_items.py

O importarse desde main.py:
    import run_catalogo_items; run_catalogo_items.run(dfs_limpios)
"""

from items_migration import build_catalogo_items_df
from items_migration.exporter_sql import export_items_to_sql
from excel_loader import load_dfs_limpios


def run(dfs_limpios: dict | None = None):
    """
    Corre la migración de catalogo_items.
    Si dfs_limpios es None, carga el Excel por su cuenta.
    """
    if dfs_limpios is None:
        dfs_limpios = load_dfs_limpios()

    df_items = build_catalogo_items_df(dfs_limpios)
    print(
        f"\nDataFrame de items listo: {df_items.shape[0]} filas x {df_items.shape[1]} columnas"
    )
    print(df_items.head(5).to_string())

    ruta_sql = export_items_to_sql(df_items, filename="catalogo_items.sql")
    print(f"\n[run_catalogo_items] SQL generado: {ruta_sql}")
    return dfs_limpios


if __name__ == "__main__":
    run()
