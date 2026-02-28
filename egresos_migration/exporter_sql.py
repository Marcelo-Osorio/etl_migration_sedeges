"""
egresos_migration.exporter_sql
===============================
Genera output/egresos.sql con INSERTs en la tabla `egresos`.
"""

import os
from datetime import datetime

import pandas as pd

from utils.logger import get_logger

logger = get_logger()

_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")


def _v(valor, quote: bool = False) -> str:
    """Formatea un valor para SQL: NULL, número o 'cadena'."""
    if valor is None:
        return "NULL"
    try:
        if pd.isna(valor):
            return "NULL"
    except Exception:
        pass
    s = str(valor).strip()
    if s in ("", "None", "nan", "NaN"):
        return "NULL"
    if quote:
        return "'" + s.replace("'", "''") + "'"
    return s


def export_egresos_to_sql(
    df: pd.DataFrame,
    filename: str = "egresos.sql",
) -> str:
    """
    Genera el archivo SQL para `egresos`.

    Args:
        df:      DataFrame de build_egresos_df()
        filename: nombre del archivo en output/

    Returns:
        Ruta absoluta del archivo generado
    """
    os.makedirs(_OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(_OUTPUT_DIR, filename)

    ahora = datetime.now()
    lineas = []

    lineas.append("-- ============================================================")
    lineas.append("-- Migración: egresos")
    lineas.append(f"-- Generado el: {ahora.strftime('%Y-%m-%d %H:%M:%S')}")
    lineas.append(f"-- Total de registros: {len(df)}")
    lineas.append("-- ============================================================")
    lineas.append("")
    lineas.append("SET NAMES utf8mb4;")
    lineas.append("SET FOREIGN_KEY_CHECKS = 0;")
    lineas.append("")
    lineas.append("-- ---- INSERT egresos ----")

    for _, row in df.iterrows():
        insert = (
            "INSERT INTO `egresos` "
            "(`ingreso_id`, `ingreso_detalle_id`, `almacen_id`, `partida_id`, `item_id`, "
            "`destino_id`, `cantidad`, `costo`, `total`, `fecha_registro`, `editable`, "
            "`created_at`, `updated_at`) "
            "VALUES ("
            f"{_v(row.get('ingreso_id'))}, "
            f"{_v(row.get('ingreso_detalle_id'))}, "
            f"{_v(row.get('almacen_id'))}, "
            f"{_v(row.get('partida_id'))}, "
            f"{_v(row.get('item_id'))}, "
            f"{_v(row.get('destino_id'))}, "
            f"{_v(row.get('cantidad'))}, "
            f"{_v(row.get('costo'))}, "
            f"{_v(row.get('total'))}, "
            f"{_v(row.get('fecha_registro'), quote=True)}, "
            f"{_v(row.get('editable'))}, "
            f"{_v(row.get('created_at'), quote=True)}, "
            f"{_v(row.get('updated_at'), quote=True)}"
            ");"
        )
        lineas.append(insert)

    lineas.append("")
    lineas.append("SET FOREIGN_KEY_CHECKS = 1;")
    lineas.append("")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lineas))

    logger.info(f"SQL generado: {output_path} ({len(df)} INSERTs)")
    return output_path
