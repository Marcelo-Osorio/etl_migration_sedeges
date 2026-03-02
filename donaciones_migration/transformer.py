"""
donaciones_migration.transformer
==================================
Construye los 3 DataFrames para la migración de DONACIONES:
  1. df_ingresos         → tabla `ingresos`
  2. df_ingreso_detalles → tabla `ingreso_detalles`
  3. df_egresos          → tabla `egresos`

Cada fila de la hoja DONACIONES del Excel produce exactamente 1 fila
en cada una de las 3 tablas, enlazadas entre sí.
"""

from datetime import datetime

import pandas as pd

from donaciones_migration.validator import ALMACEN_ID_DONACION, USER_ID_ADMIN
from utils.logger import get_logger

logger = get_logger()

_COL_INGRESO = "INGRESO EN LA GESTION 2025"
_COL_SALIDA = "SALIDA EN LA GESTION 2025"


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


def build_donaciones_dfs(
    df_donaciones: pd.DataFrame,
    partida_ids: list[int],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Construye los 3 DataFrames alineados 1:1 con las filas de DONACIONES.

    Args:
        df_donaciones: DataFrame limpio de la hoja DONACIONES
        partida_ids:   lista de partida_id ya resueltos por el validator

    Returns:
        (df_ingresos, df_ingreso_detalles, df_egresos)
    """
    ahora = datetime.now()
    fecha_registro = ahora.strftime("%Y-%m-%d")
    created_at = ahora.strftime("%Y-%m-%d %H:%M:%S")
    updated_at = ahora.strftime("%Y-%m-%d %H:%M:%S")

    n = len(df_donaciones)

    totales_ingreso = [
        _to_num(df_donaciones.iloc[i].get(_COL_INGRESO), 0) for i in range(n)
    ]
    totales_salida = [
        _to_num(df_donaciones.iloc[i].get(_COL_SALIDA), 0) for i in range(n)
    ]

    # ---- DataFrame 1: ingresos ----
    df_ingresos = pd.DataFrame(
        {
            "codigo": ["XXX"] * n,
            "donacion": ["SI"] * n,
            "almacen_id": [ALMACEN_ID_DONACION] * n,
            "unidad_id": [None] * n,
            "proveedor": [None] * n,
            "con_fondos": [None] * n,
            "fecha_nota": [None] * n,
            "nro_factura": [None] * n,
            "fecha_factura": [None] * n,
            "pedido_interno": [None] * n,
            "total": totales_ingreso,
            "fecha_ingreso": [None] * n,
            "hora_ingreso": [None] * n,
            "observaciones": [None] * n,
            "para": [None] * n,
            "fecha_registro": [fecha_registro] * n,
            "user_id": [USER_ID_ADMIN] * n,
            "created_at": [created_at] * n,
            "updated_at": [updated_at] * n,
            "etapa_ingreso": ["2025"] * n,
        }
    )

    # ---- DataFrame 2: ingreso_detalles ----
    df_ingreso_detalles = pd.DataFrame(
        {
            "_pos": list(range(n)),
            "almacen_id": [ALMACEN_ID_DONACION] * n,
            "unidad_id": [None] * n,
            "partida_id": partida_ids,
            "donacion": ["SI"] * n,
            "item_id": [None] * n,
            "unidad_medida_id": [None] * n,
            "cantidad": [None] * n,
            "costo": [None] * n,
            "total": totales_ingreso,
            "created_at": [created_at] * n,
            "updated_at": [updated_at] * n,
        }
    )

    # ---- DataFrame 3: egresos ----
    df_egresos = pd.DataFrame(
        {
            "_pos": list(range(n)),
            "almacen_id": [ALMACEN_ID_DONACION] * n,
            "partida_id": partida_ids,
            "item_id": [None] * n,
            "destino_id": [None] * n,
            "cantidad": [None] * n,
            "costo": [None] * n,
            "total": totales_salida,
            "fecha_registro": [fecha_registro] * n,
            "editable": [1] * n,
            "created_at": [created_at] * n,
            "updated_at": [updated_at] * n,
        }
    )

    logger.info(
        f"DataFrames DONACIONES construidos: {n} filas "
        f"(ingresos={len(df_ingresos)}, "
        f"ingreso_detalles={len(df_ingreso_detalles)}, "
        f"egresos={len(df_egresos)})"
    )

    return df_ingresos, df_ingreso_detalles, df_egresos
