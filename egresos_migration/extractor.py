"""
egresos_migration.extractor
===========================

Este módulo SOLO se encarga de "sacar" datos de:
  - Los DataFrames limpios del Excel (dfs_limpios)
  - La base de datos MySQL

No hace cálculos de negocio, solo prepara tres cosas:
  1) Un DataFrame con todas las SALIDAS del Excel (por almacén)
  2) Un DataFrame con ingreso_detalles (id > 7)
  3) Un diccionario item_id -> nombre de catalogo_items
"""

import json
import os

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

_REL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "utils", "tables.db.relation.json"
)
_ORG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "utils", "tables.excel.organization.json"
)

with open(_REL_PATH, encoding="utf-8") as _f:
    _RELACION = json.load(_f)

with open(_ORG_PATH, encoding="utf-8") as _f:
    _ORG = json.load(_f)

HOJAS_DETALLE: set = set(_ORG["detalles"].keys())
# Relación nombre_hoja -> id de almacén, sacada del JSON de utilidades
_ALMACEN_MAP: dict = {k: v["almacen_id"] for k, v in _RELACION["detalles"].items()}

_COLS_SALIDA = [
    "almacen_id",
    "hoja_origen",
    "DESCRIPCION",
    "SALIDA_ALMACENES_CANT",
    "SALIDA_ALMACENES_VALOR",
    "SALIDA_ALMACENES_TOTAL Bs.",
]


def build_df_limpio_unificado(dfs_limpios: dict) -> pd.DataFrame:
    """
    Une TODAS las hojas detalle del Excel en un solo DataFrame.

    Columnas de salida:
      - almacen_id      (según el JSON de relaciones)
      - hoja_origen     (nombre de la hoja en el Excel)
      - DESCRIPCION
      - SALIDA_ALMACENES_CANT
      - SALIDA_ALMACENES_VALOR
      - SALIDA_ALMACENES_TOTAL Bs.

    Si alguna hoja no tiene estas columnas, simplemente se ignora.
    """
    required = ["DESCRIPCION", "SALIDA_ALMACENES_CANT", "SALIDA_ALMACENES_VALOR", "SALIDA_ALMACENES_TOTAL Bs."]
    fragmentos = []
    for nombre_hoja, df in dfs_limpios.items():
        if nombre_hoja not in HOJAS_DETALLE:
            continue
        if not all(c in df.columns for c in required):
            continue
        fragmento = df[required].copy()
        fragmento["hoja_origen"] = nombre_hoja
        fragmento["almacen_id"] = _ALMACEN_MAP.get(nombre_hoja)
        fragmentos.append(fragmento)

    if not fragmentos:
        return pd.DataFrame()

    return pd.concat(fragmentos, ignore_index=True)


def fetch_ingreso_detalles_gt7(engine: Engine) -> pd.DataFrame:
    """
    Lee desde la DB la tabla ingreso_detalles, pero SOLO los registros
    con id > 7 (los que vienen de la migración actual).

    Devuelve un DataFrame con las columnas:
      id, ingreso_id, almacen_id, partida_id, item_id
    en orden por id (ASC).
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, ingreso_id, almacen_id, partida_id, item_id "
                "FROM ingreso_detalles WHERE id > 7 ORDER BY id ASC"
            )
        ).fetchall()
    return pd.DataFrame(
        rows,
        columns=["id", "ingreso_id", "almacen_id", "partida_id", "item_id"],
    )


def fetch_catalogo_items_nombres(engine: Engine) -> dict[int, str]:
    """
    Devuelve un diccionario muy simple:
        { item_id: nombre }

    Lo usamos para comparar DESCRIPCION (Excel) con nombre (DB),
    siempre en minúsculas.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, nombre FROM catalogo_items")
        ).fetchall()
    return {int(r[0]): (r[1] or "").strip() for r in rows}
