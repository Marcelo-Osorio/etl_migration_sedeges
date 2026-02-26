"""
ingreso_detalles_migration
==========================
Módulo de migración ETL para la tabla `ingreso_detalles`.

Genera un registro de detalle por **cada fila de datos** de todas las
hojas detalle del Excel. Realiza merges con `partidas`, `catalogo_items`
y `unidad_medidas` de la DB (con inserción automática si no hay match).

Uso:
    from ingreso_detalles_migration import build_ingreso_detalles_df
    df, etapas = build_ingreso_detalles_df(dfs_limpios, engine)
"""

from ingreso_detalles_migration.transformer import build_ingreso_detalles_df

__all__ = ["build_ingreso_detalles_df"]
