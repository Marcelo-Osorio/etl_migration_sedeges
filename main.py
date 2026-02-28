"""
main.py
=======
Orquestador principal de la migración ETL de SEDEGES.

Ejecuta en orden:
  1. catalogo_items → output/catalogo_items.sql
  2. ingresos       → output/ingresos.sql
  3. ingreso_detalles → output/ingreso_detalles.sql
                        (+ renames columna producto_id → item_id en DB)
  4. egresos        → output/egresos.sql

Uso:
    python main.py
"""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

import run_catalogo_items
import run_egresos
import run_ingresos
import run_ingreso_detalles

load_dotenv()


def _get_engine():
    url = (
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '3306')}"
        f"/{os.getenv('DB_NAME')}"
    )
    return create_engine(url)


def main():
    print("=" * 60)
    print("ETL SEDEGES — Pipeline completa")
    print("=" * 60)

    # Cargar el Excel una sola vez y reutilizarlo
    dfs_limpios = run_catalogo_items.load_dfs_limpios()

    # 1. catalogo_items
    run_catalogo_items.run(dfs_limpios)

    # 2. ingresos
    run_ingresos.run(dfs_limpios)

    # 3. ingreso_detalles (necesita la DB para lookups y renombrar columna)
    engine = _get_engine()
    run_ingreso_detalles.run(dfs_limpios, engine)

    # 4. egresos (requiere ingreso_detalles con id > 7 en la DB)
    run_egresos.run(dfs_limpios, engine)

    print("\n" + "=" * 60)
    print("Pipeline completa. Archivos generados en output/:")
    print("  - catalogo_items.sql")
    print("  - ingresos.sql")
    print("  - ingreso_detalles.sql")
    print("  - egresos.sql")
    print("=" * 60)


if __name__ == "__main__":
    main()
