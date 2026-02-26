"""
ingresos_migration
==================
Módulo de migración ETL para la tabla `ingresos`.

Genera un registro de ingreso por **cada fila de datos** de todas las
hojas detalle del Excel. El almacen_id se toma del mapeo definido en
utils/tables.db.relation.json según la hoja de origen.

Uso:
    from ingresos_migration import build_ingresos_df
    df = build_ingresos_df(dfs_limpios)
"""

from ingresos_migration.transformer import build_ingresos_df

__all__ = ["build_ingresos_df"]
