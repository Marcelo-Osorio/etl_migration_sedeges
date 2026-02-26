"""
ingreso_detalles_migration.extractor
=====================================
Extrae y enriquece el DataFrame de todas las hojas detalle para
la migración de `ingreso_detalles`.

Realiza merges con las tablas de la DB:
  - partidas       (PARTIDA_CODIGO ↔ nro_partida)
  - catalogo_items (DESCRIPCION    ↔ nombre)
  - unidad_medidas (UNIDAD         ↔ nombre)

Si algún valor no tiene par en la DB, se inserta automáticamente
y se registra un log de advertencia.
"""

import json
import os
from datetime import datetime, date

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

# ------------------------------------------------------------------ #
# Configuración                                                        #
# ------------------------------------------------------------------ #
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
_ALMACEN_MAP: dict = {k: v["almacen_id"] for k, v in _RELACION["detalles"].items()}

_NOW_STR = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
_DATE_STR = datetime.now().strftime("%Y-%m-%d")


# ------------------------------------------------------------------ #
# Helpers de fallback                                                  #
# ------------------------------------------------------------------ #


def _ensure_catalogo_item(nombre: str, engine: Engine) -> int:
    """
    Busca el nombre en catalogo_items (normalizado).
    Si no existe, lo inserta y retorna el nuevo ID.
    """
    nombre_norm = nombre.strip().lower()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT id FROM catalogo_items WHERE LOWER(TRIM(nombre)) = :n LIMIT 1"
            ),
            {"n": nombre_norm},
        ).fetchone()
        if row:
            return row[0]
        # No encontrado → insertar
        result = conn.execute(
            text(
                "INSERT INTO catalogo_items (nombre, fecha_registro, created_at, updated_at) "
                "VALUES (:nombre, :fr, :ca, :ua)"
            ),
            {
                "nombre": nombre.strip(),
                "fr": _DATE_STR,
                "ca": _NOW_STR,
                "ua": _NOW_STR,
            },
        )
        conn.commit()
        new_id = result.lastrowid
        print(
            f"[WARN] catalogo_items: '{nombre_norm}' no encontrado "
            f"→ insertando en DB y extrayendo id={new_id}"
        )
        return new_id


def _ensure_unidad_medida(nombre: str, engine: Engine) -> int:
    """
    Busca el nombre en unidad_medidas (normalizado).
    Si no existe, lo inserta y retorna el nuevo ID.
    """
    nombre_norm = nombre.strip().lower()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT id FROM unidad_medidas WHERE LOWER(TRIM(nombre)) = :n LIMIT 1"
            ),
            {"n": nombre_norm},
        ).fetchone()
        if row:
            return row[0]
        result = conn.execute(
            text(
                "INSERT INTO unidad_medidas (nombre, abreviatura, fecha_registro, created_at, updated_at) "
                "VALUES (:nombre, :abr, :fr, :ca, :ua)"
            ),
            {
                "nombre": nombre.strip(),
                "abr": nombre.strip()[:10],
                "fr": _DATE_STR,
                "ca": _NOW_STR,
                "ua": _NOW_STR,
            },
        )
        conn.commit()
        new_id = result.lastrowid
        print(
            f"[WARN] unidad_medidas: '{nombre_norm}' no encontrado "
            f"→ insertando en DB y extrayendo id={new_id}"
        )
        return new_id


def _ensure_partida(nro: str, engine: Engine) -> int | None:
    """
    Busca el nro_partida en partidas (normalizado).
    Si no existe, lo inserta y retorna el nuevo ID.
    Retorna None si nro es nulo/vacío.
    """
    if pd.isna(nro) or str(nro).strip() == "":
        return None
    nro_norm = str(nro).strip().lower()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id FROM partidas WHERE LOWER(TRIM(nro_partida)) = :n LIMIT 1"),
            {"n": nro_norm},
        ).fetchone()
        if row:
            return row[0]
        result = conn.execute(
            text(
                "INSERT INTO partidas (nro_partida, nombre, fecha_registro, created_at, updated_at) "
                "VALUES (:nro, :nombre, :fr, :ca, :ua)"
            ),
            {
                "nro": str(nro).strip(),
                "nombre": f"Partida {str(nro).strip()}",
                "fr": _DATE_STR,
                "ca": _NOW_STR,
                "ua": _NOW_STR,
            },
        )
        conn.commit()
        new_id = result.lastrowid
        print(
            f"[WARN] partidas: '{nro_norm}' no encontrado "
            f"→ insertando en DB y extrayendo id={new_id}"
        )
        return new_id


# ------------------------------------------------------------------ #
# Función principal                                                    #
# ------------------------------------------------------------------ #


def extract_ingreso_detalles(dfs_limpios: dict, engine: Engine) -> pd.DataFrame:
    """
    Extrae y enriquece todas las filas de las hojas detalle para
    la migración de `ingreso_detalles`.

    Args:
        dfs_limpios: dict {nombre_hoja: DataFrame} limpios por rules.py
        engine:      SQLAlchemy engine conectado a la DB

    Returns:
        DataFrame enriquecido con columnas:
            hoja_origen, almacen_id, PARTIDA_CODIGO, DESCRIPCION, UNIDAD,
            SALDO_AL_01_DE_ENERO_DE_2025_CANT, SALDO_AL_01_DE_ENERO_DE_2025_valor,
            SALDO_AL_01_DE_ENERO_DE_2025_TOTAL Bs.,
            INGRESO_ALMACENES_CANT, INGRESO_ALMACENES_VALOR, INGRESO_ALMACENES_TOTAL Bs.,
            partida_id, item_id, unidad_medida_id
    """
    fragmentos = []

    for nombre_hoja, df in dfs_limpios.items():
        if nombre_hoja not in HOJAS_DETALLE:
            continue
        fragmento = df.copy()
        fragmento["hoja_origen"] = nombre_hoja
        fragmento["almacen_id"] = _ALMACEN_MAP.get(nombre_hoja)
        fragmentos.append(fragmento)

    if not fragmentos:
        return pd.DataFrame()

    df_all = pd.concat(fragmentos, ignore_index=True)
    print(
        f"[ingreso_detalles_migration] Total filas extraídas de hojas detalle: {len(df_all)}"
    )

    # ---- Resolver partida_id ----------------------------------------
    print("[ingreso_detalles_migration] Resolviendo partida_id...")
    partida_ids = []
    for _, row in df_all.iterrows():
        nro = row.get("PARTIDA_CODIGO")
        partida_ids.append(_ensure_partida(nro, engine))
    df_all["partida_id"] = partida_ids

    # ---- Resolver item_id (catalogo_items) --------------------------
    print("[ingreso_detalles_migration] Resolviendo item_id (catalogo_items)...")
    item_ids = []
    for _, row in df_all.iterrows():
        desc = row.get("DESCRIPCION")
        if pd.isna(desc) or str(desc).strip() == "":
            item_ids.append(None)
        else:
            item_ids.append(_ensure_catalogo_item(str(desc), engine))
    df_all["item_id"] = item_ids

    # ---- Resolver unidad_medida_id ----------------------------------
    print(
        "[ingreso_detalles_migration] Resolviendo unidad_medida_id (unidad_medidas)..."
    )
    unidad_ids = []
    for _, row in df_all.iterrows():
        unidad = row.get("UNIDAD")
        if pd.isna(unidad) or str(unidad).strip() == "":
            # Fallback si no hay unidad: insertar 'DESCONOCIDO'
            unidad_ids.append(_ensure_unidad_medida("DESCONOCIDO", engine))
        else:
            unidad_ids.append(_ensure_unidad_medida(str(unidad), engine))
    df_all["unidad_medida_id"] = unidad_ids

    print(
        f"[ingreso_detalles_migration] Extracción y enriquecimiento completo: {len(df_all)} filas"
    )
    return df_all
