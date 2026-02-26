"""
ingresos_migration.transformer
===============================
Construye el DataFrame para la tabla `ingresos`.
Genera una fila por cada registro de datos de todas las hojas detalle.
"""

import json
import os
from datetime import datetime, date

import pandas as pd

# ------------------------------------------------------------------ #
# Carga el mapeo hoja → almacen_id                                   #
# ------------------------------------------------------------------ #
_REL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "utils", "tables.db.relation.json"
)
with open(_REL_PATH, encoding="utf-8") as _f:
    _RELACION: dict = json.load(_f)

_DETALLES_ALMACEN: dict = _RELACION.get("detalles", {})

# Hojas que son de tipo detalle
HOJAS_DETALLE: set = set(_DETALLES_ALMACEN.keys())


def _parse_fecha(valor) -> str | None:
    """
    Intenta parsear una fecha en múltiples formatos.
    Retorna 'YYYY-MM-DD' o None si no puede interpretar el valor.
    """
    if pd.isna(valor):
        return None
    # Cuando pandas ya cargó como datetime
    if isinstance(valor, (datetime, date, pd.Timestamp)):
        try:
            ts = pd.Timestamp(valor)
            if pd.isna(ts):
                return None
            return ts.strftime("%Y-%m-%d")
        except Exception:
            return None
    # Intentar parsear desde string
    try:
        ts = pd.to_datetime(str(valor).strip(), dayfirst=True, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.strftime("%Y-%m-%d")
    except Exception:
        return None


def build_ingresos_df(dfs_limpios: dict) -> pd.DataFrame:
    """
    Recorre todas las hojas detalle y construye el DataFrame de `ingresos`.

    Una fila de `ingresos` por cada fila de datos de todas las hojas detalle.
    El almacen_id se toma del mapeo en tables.db.relation.json.

    Args:
        dfs_limpios: dict {nombre_hoja: DataFrame} ya limpiados por rules.py

    Returns:
        DataFrame con columnas listas para INSERT en `ingresos`
    """
    ahora = datetime.now()
    fecha_registro = ahora.strftime("%Y-%m-%d")
    created_at = ahora.strftime("%Y-%m-%d %H:%M:%S")
    updated_at = ahora.strftime("%Y-%m-%d %H:%M:%S")

    fragmentos = []

    for nombre_hoja, df in dfs_limpios.items():
        if nombre_hoja not in HOJAS_DETALLE:
            continue

        almacen_id = _DETALLES_ALMACEN[nombre_hoja]["almacen_id"]
        n = len(df)

        # Extraer y normalizar FECHA INGRESO fila a fila
        if "FECHA INGRESO" in df.columns:
            fechas = df["FECHA INGRESO"].apply(_parse_fecha)
        else:
            fechas = pd.Series([None] * n)

        fragmento = pd.DataFrame(
            {
                "codigo": ["XXX"] * n,
                "donacion": ["NO"] * n,
                "almacen_id": [almacen_id] * n,
                "unidad_id": [None] * n,
                "proveedor": [None] * n,
                "con_fondos": [None] * n,
                "fecha_nota": [None] * n,
                "nro_factura": [None] * n,
                "fecha_factura": [None] * n,
                "pedido_interno": [None] * n,
                "total": [1] * n,
                "fecha_ingreso": fechas.values,
                "hora_ingreso": [None] * n,
                "observaciones": [None] * n,
                "para": [None] * n,
                "fecha_registro": [fecha_registro] * n,
                "user_id": [1] * n,
                "created_at": [created_at] * n,
                "updated_at": [updated_at] * n,
                "etapa_ingreso": [None] * n,
                "_hoja_origen": [nombre_hoja] * n,
            }
        )
        fragmentos.append(fragmento)

    if not fragmentos:
        return pd.DataFrame()

    df_final = pd.concat(fragmentos, ignore_index=True)
    print(
        f"[ingresos_migration] Total filas construidas para `ingresos`: {len(df_final)}"
    )
    return df_final
