"""
donaciones_migration.exporter_sql
==================================
Genera output/donaciones.sql con las 3 tablas encadenadas usando
variables MySQL (@ingreso_id, @detalle_id = LAST_INSERT_ID()).

Cada fila de DONACIONES produce un bloque:
  INSERT ingresos     → SET @ingreso_id = LAST_INSERT_ID();
  INSERT ingreso_det  → SET @detalle_id = LAST_INSERT_ID();
  INSERT egresos      (usa @ingreso_id y @detalle_id)
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


def export_donaciones_to_sql(
    df_ingresos: pd.DataFrame,
    df_ingreso_detalles: pd.DataFrame,
    df_egresos: pd.DataFrame,
    filename: str = "donaciones.sql",
) -> str:
    """
    Genera un único archivo SQL con INSERTs encadenados para las 3 tablas.

    Args:
        df_ingresos:          DataFrame de ingresos DONACIONES
        df_ingreso_detalles:  DataFrame de ingreso_detalles DONACIONES
        df_egresos:           DataFrame de egresos DONACIONES
        filename:             nombre del archivo en output/

    Returns:
        Ruta absoluta del archivo generado
    """
    os.makedirs(_OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(_OUTPUT_DIR, filename)

    ahora = datetime.now()
    n = len(df_ingresos)
    lineas: list[str] = []

    lineas.append("-- ============================================================")
    lineas.append("-- Migración: DONACIONES (ingresos + ingreso_detalles + egresos)")
    lineas.append(f"-- Generado el: {ahora.strftime('%Y-%m-%d %H:%M:%S')}")
    lineas.append(f"-- Total de filas DONACIONES: {n}")
    lineas.append("-- Cada fila genera 1 ingreso + 1 ingreso_detalle + 1 egreso")
    lineas.append("-- ============================================================")
    lineas.append("")
    lineas.append("SET NAMES utf8mb4;")
    lineas.append("SET FOREIGN_KEY_CHECKS = 0;")
    lineas.append("")

    for i in range(n):
        ing = df_ingresos.iloc[i]
        det = df_ingreso_detalles.iloc[i]
        egr = df_egresos.iloc[i]

        lineas.append(f"-- ---- Fila {i + 1} / {n} ----")

        # ---- INSERT ingresos ----
        lineas.append(
            "INSERT INTO `ingresos` "
            "(`codigo`, `donacion`, `almacen_id`, `unidad_id`, `proveedor`, "
            "`con_fondos`, `fecha_nota`, `nro_factura`, `fecha_factura`, "
            "`pedido_interno`, `total`, `fecha_ingreso`, `hora_ingreso`, "
            "`observaciones`, `para`, `fecha_registro`, `user_id`, "
            "`created_at`, `updated_at`, `etapa_ingreso`) "
            "VALUES ("
            f"{_v(ing.get('codigo'), quote=True)}, "
            f"{_v(ing.get('donacion'), quote=True)}, "
            f"{_v(ing.get('almacen_id'))}, "
            f"{_v(ing.get('unidad_id'))}, "
            f"{_v(ing.get('proveedor'), quote=True)}, "
            f"{_v(ing.get('con_fondos'), quote=True)}, "
            f"{_v(ing.get('fecha_nota'), quote=True)}, "
            f"{_v(ing.get('nro_factura'), quote=True)}, "
            f"{_v(ing.get('fecha_factura'), quote=True)}, "
            f"{_v(ing.get('pedido_interno'), quote=True)}, "
            f"{_v(ing.get('total'))}, "
            f"{_v(ing.get('fecha_ingreso'), quote=True)}, "
            f"{_v(ing.get('hora_ingreso'), quote=True)}, "
            f"{_v(ing.get('observaciones'), quote=True)}, "
            f"{_v(ing.get('para'), quote=True)}, "
            f"{_v(ing.get('fecha_registro'), quote=True)}, "
            f"{_v(ing.get('user_id'))}, "
            f"{_v(ing.get('created_at'), quote=True)}, "
            f"{_v(ing.get('updated_at'), quote=True)}, "
            f"{_v(ing.get('etapa_ingreso'), quote=True)}"
            ");"
        )
        lineas.append("SET @ingreso_id = LAST_INSERT_ID();")
        lineas.append("")

        # ---- INSERT ingreso_detalles ----
        lineas.append(
            "INSERT INTO `ingreso_detalles` "
            "(`ingreso_id`, `almacen_id`, `unidad_id`, `partida_id`, `donacion`, "
            "`item_id`, `unidad_medida_id`, `cantidad`, `costo`, `total`, "
            "`created_at`, `updated_at`) "
            "VALUES ("
            "@ingreso_id, "
            f"{_v(det.get('almacen_id'))}, "
            f"{_v(det.get('unidad_id'))}, "
            f"{_v(det.get('partida_id'))}, "
            f"{_v(det.get('donacion'), quote=True)}, "
            f"{_v(det.get('item_id'))}, "
            f"{_v(det.get('unidad_medida_id'))}, "
            f"{_v(det.get('cantidad'))}, "
            f"{_v(det.get('costo'))}, "
            f"{_v(det.get('total'))}, "
            f"{_v(det.get('created_at'), quote=True)}, "
            f"{_v(det.get('updated_at'), quote=True)}"
            ");"
        )
        lineas.append("SET @detalle_id = LAST_INSERT_ID();")
        lineas.append("")

        # ---- INSERT egresos ----
        lineas.append(
            "INSERT INTO `egresos` "
            "(`ingreso_id`, `ingreso_detalle_id`, `almacen_id`, `partida_id`, "
            "`item_id`, `destino_id`, `cantidad`, `costo`, `total`, "
            "`fecha_registro`, `editable`, `created_at`, `updated_at`) "
            "VALUES ("
            "@ingreso_id, "
            "@detalle_id, "
            f"{_v(egr.get('almacen_id'))}, "
            f"{_v(egr.get('partida_id'))}, "
            f"{_v(egr.get('item_id'))}, "
            f"{_v(egr.get('destino_id'))}, "
            f"{_v(egr.get('cantidad'))}, "
            f"{_v(egr.get('costo'))}, "
            f"{_v(egr.get('total'))}, "
            f"{_v(egr.get('fecha_registro'), quote=True)}, "
            f"{_v(egr.get('editable'))}, "
            f"{_v(egr.get('created_at'), quote=True)}, "
            f"{_v(egr.get('updated_at'), quote=True)}"
            ");"
        )
        lineas.append("")

    lineas.append("SET FOREIGN_KEY_CHECKS = 1;")
    lineas.append("")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lineas))

    logger.info(
        f"SQL generado: {output_path} "
        f"({n} ingresos + {n} ingreso_detalles + {n} egresos = {n * 3} INSERTs)"
    )
    return output_path
