"""
ingreso_detalles_migration.transformer
=======================================
Construye el DataFrame final para la tabla `ingreso_detalles`.

Lógica principal:
  - Si SALDO_AL_01_DE_ENERO_DE_2025_TOTAL Bs. > 0:
        cantidad/costo/total = columnas de SALDO  → etapa = 'ANTES 2025'
  - Sino:
        cantidad/costo/total = columnas de INGRESO → etapa = 'DESPUES 2025'

El ingreso_id y almacen_id se toman de los registros recién insertados
en la tabla `ingresos` (id > 6).
"""

from datetime import datetime

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from ingreso_detalles_migration.extractor import extract_ingreso_detalles

_COL_SALDO_TOTAL = "SALDO_AL_01_DE_ENERO_DE_2025_TOTAL Bs."
_COL_SALDO_CANT = "SALDO_AL_01_DE_ENERO_DE_2025_CANT"
_COL_SALDO_VALOR = "SALDO_AL_01_DE_ENERO_DE_2025_valor"

_COL_ING_TOTAL = "INGRESO_ALMACENES_TOTAL Bs."
_COL_ING_CANT = "INGRESO_ALMACENES_CANT"
_COL_ING_VALOR = "INGRESO_ALMACENES_VALOR"


def _to_num(val, default=0):
    """Convierte a número; retorna default si no es posible."""
    try:
        if pd.isna(val):
            return default
    except Exception:
        pass
    try:
        return float(str(val).replace(",", ".").strip())
    except (ValueError, TypeError):
        return default


def _fetch_new_ingresos(engine: Engine) -> pd.DataFrame:
    """
    Trae de la DB los registros de `ingresos` con id > 6
    (los recién insertados en la migración).
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, almacen_id FROM ingresos WHERE id > 6 ORDER BY id ASC")
        ).fetchall()
    df = pd.DataFrame(rows, columns=["id", "almacen_id"])
    print(
        f"[ingreso_detalles_migration] Registros nuevos en `ingresos` (id>6): {len(df)}"
    )
    return df


def build_ingreso_detalles_df(
    dfs_limpios: dict, engine: Engine
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Construye el DataFrame para `ingreso_detalles`.

    Returns:
        (df_detalles, etapas_series)
        - df_detalles: DataFrame listo para INSERT en ingreso_detalles
        - etapas_series: Series con (ingreso_id, etapa) para UPDATE de ingresos
    """
    ahora = datetime.now()
    created_at = ahora.strftime("%Y-%m-%d %H:%M:%S")
    updated_at = ahora.strftime("%Y-%m-%d %H:%M:%S")

    # Paso 1: Extraer + enriquecer con IDs de DB
    df_raw = extract_ingreso_detalles(dfs_limpios, engine)

    # Paso 2: Traer los ingresos recién insertados
    df_ingresos = _fetch_new_ingresos(engine)

    if len(df_ingresos) != len(df_raw):
        print(
            f"[WARN] Filas de detalle ({len(df_raw)}) ≠ "
            f"ingresos nuevos ({len(df_ingresos)}). "
            "Verificar que ingresos.sql fue ejecutado antes."
        )

    # Alinear por posición (mismo orden de inserción)
    n = min(len(df_raw), len(df_ingresos))
    df_raw = df_raw.iloc[:n].reset_index(drop=True)
    df_ingresos = df_ingresos.iloc[:n].reset_index(drop=True)

    # Paso 3: Calcular cantidad/costo/total/etapa con la condicional
    cantidades, costos, totales, etapas = [], [], [], []

    for _, row in df_raw.iterrows():
        saldo_total = _to_num(row.get(_COL_SALDO_TOTAL), 0)

        if saldo_total > 0:
            cantidades.append(_to_num(row.get(_COL_SALDO_CANT), 0))
            costos.append(_to_num(row.get(_COL_SALDO_VALOR), 0))
            totales.append(saldo_total)
            etapas.append("ANTES 2025")
        else:
            cantidades.append(_to_num(row.get(_COL_ING_CANT), 0))
            costos.append(_to_num(row.get(_COL_ING_VALOR), 0))
            totales.append(_to_num(row.get(_COL_ING_TOTAL), 0))
            etapas.append("2025")

    # Paso 4: Construir DataFrame final
    df_final = pd.DataFrame(
        {
            "ingreso_id": df_ingresos["id"].values,
            "almacen_id": df_ingresos["almacen_id"].values,
            "unidad_id": [None] * n,
            "partida_id": df_raw["partida_id"].values,
            "donacion": ["NO"] * n,
            "item_id": df_raw["item_id"].values,
            "unidad_medida_id": df_raw["unidad_medida_id"].values,
            "cantidad": cantidades,
            "costo": costos,
            "total": totales,
            "created_at": [created_at] * n,
            "updated_at": [updated_at] * n,
            "_etapa": etapas,
        }
    )

    print(f"[ingreso_detalles_migration] DataFrame final: {len(df_final)} filas")
    etapa_counts = pd.Series(etapas).value_counts()
    print(
        f"[ingreso_detalles_migration] Distribución etapas:\n{etapa_counts.to_string()}"
    )

    return df_final, df_final[["ingreso_id", "_etapa"]].copy()
