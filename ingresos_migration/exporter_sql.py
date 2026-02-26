"""
ingresos_migration.exporter_sql
================================
Exporta el DataFrame de `ingresos` a output/ingresos.sql
"""

import os
from datetime import datetime

import pandas as pd

_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")


def _v(valor, quote: bool = False) -> str:
    """Formatea un valor para SQL: NULL o 'valor' o número."""
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return "NULL"
    s = str(valor).strip()
    if s == "" or s == "None" or s == "nan":
        return "NULL"
    if quote:
        return "'" + s.replace("'", "''") + "'"
    return s


def export_ingresos_to_sql(df: pd.DataFrame, filename: str = "ingresos.sql") -> str:
    """
    Genera el archivo SQL con INSERTs para la tabla `ingresos`.

    Args:
        df:       DataFrame construido por ingresos_migration.transformer
        filename: nombre de archivo destino en output/

    Returns:
        Ruta absoluta del archivo generado
    """
    os.makedirs(_OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(_OUTPUT_DIR, filename)

    ahora = datetime.now()
    lineas = []

    lineas.append("-- ============================================================")
    lineas.append("-- Migración: ingresos")
    lineas.append(f"-- Generado el: {ahora.strftime('%Y-%m-%d %H:%M:%S')}")
    lineas.append(f"-- Total de registros: {len(df)}")
    lineas.append("-- Una fila por cada registro de datos de las hojas detalle")
    lineas.append("-- ============================================================")
    lineas.append("")
    lineas.append("SET NAMES utf8mb4;")
    lineas.append("SET FOREIGN_KEY_CHECKS = 0;")
    lineas.append("")

    for _, row in df.iterrows():
        insert = (
            "INSERT INTO `ingresos` "
            "(`codigo`, `donacion`, `almacen_id`, `unidad_id`, `proveedor`, "
            "`con_fondos`, `fecha_nota`, `nro_factura`, `fecha_factura`, "
            "`pedido_interno`, `total`, `fecha_ingreso`, `hora_ingreso`, "
            "`observaciones`, `para`, `fecha_registro`, `user_id`, "
            "`created_at`, `updated_at`, `etapa_ingreso`) "
            "VALUES ("
            f"{_v(row.get('codigo'), quote=True)}, "
            f"{_v(row.get('donacion'), quote=True)}, "
            f"{_v(row.get('almacen_id'))}, "
            f"{_v(row.get('unidad_id'))}, "
            f"{_v(row.get('proveedor'), quote=True)}, "
            f"{_v(row.get('con_fondos'), quote=True)}, "
            f"{_v(row.get('fecha_nota'), quote=True)}, "
            f"{_v(row.get('nro_factura'), quote=True)}, "
            f"{_v(row.get('fecha_factura'), quote=True)}, "
            f"{_v(row.get('pedido_interno'), quote=True)}, "
            f"{_v(row.get('total'))}, "
            f"{_v(row.get('fecha_ingreso'), quote=True)}, "
            f"{_v(row.get('hora_ingreso'), quote=True)}, "
            f"{_v(row.get('observaciones'), quote=True)}, "
            f"{_v(row.get('para'), quote=True)}, "
            f"{_v(row.get('fecha_registro'), quote=True)}, "
            f"{_v(row.get('user_id'))}, "
            f"{_v(row.get('created_at'), quote=True)}, "
            f"{_v(row.get('updated_at'), quote=True)}, "
            f"{_v(row.get('etapa_ingreso'), quote=True)}"
            ");"
        )
        lineas.append(insert)

    lineas.append("")
    lineas.append("SET FOREIGN_KEY_CHECKS = 1;")
    lineas.append("")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lineas))

    print(f"[ingresos_migration] SQL generado: {output_path} ({len(df)} registros)")
    return output_path
