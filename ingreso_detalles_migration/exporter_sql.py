"""
ingreso_detalles_migration.exporter_sql
=========================================
Genera output/ingreso_detalles.sql con:
  1. INSERTs en `ingreso_detalles`
  2. UPDATEs de `ingresos.total` (suma de totales por ingreso_id)
  3. UPDATEs de `ingresos.etapa_ingreso` ('ANTES 2025' | 'DESPUES 2025')
"""

import os
from datetime import datetime

import pandas as pd

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


def export_ingreso_detalles_to_sql(
    df: pd.DataFrame,
    etapas_df: pd.DataFrame,
    filename: str = "ingreso_detalles.sql",
) -> str:
    """
    Genera el archivo SQL para `ingreso_detalles`.

    Args:
        df:        DataFrame de build_ingreso_detalles_df()
        etapas_df: DataFrame con columnas [ingreso_id, _etapa]
        filename:  nombre del archivo en output/

    Returns:
        Ruta absoluta del archivo generado
    """
    os.makedirs(_OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(_OUTPUT_DIR, filename)

    ahora = datetime.now()
    lineas = []

    lineas.append("-- ============================================================")
    lineas.append("-- Migración: ingreso_detalles")
    lineas.append(f"-- Generado el: {ahora.strftime('%Y-%m-%d %H:%M:%S')}")
    lineas.append(f"-- Total de registros: {len(df)}")
    lineas.append("-- ============================================================")
    lineas.append("")
    lineas.append("SET NAMES utf8mb4;")
    lineas.append("SET FOREIGN_KEY_CHECKS = 0;")
    lineas.append("")

    # ---- 1. INSERTs ingreso_detalles --------------------------------
    lineas.append("-- ---- INSERT ingreso_detalles ----")
    for _, row in df.iterrows():
        insert = (
            "INSERT INTO `ingreso_detalles` "
            "(`ingreso_id`, `almacen_id`, `unidad_id`, `partida_id`, `donacion`, "
            "`item_id`, `unidad_medida_id`, `cantidad`, `costo`, `total`, "
            "`created_at`, `updated_at`) "
            "VALUES ("
            f"{_v(row.get('ingreso_id'))}, "
            f"{_v(row.get('almacen_id'))}, "
            f"{_v(row.get('unidad_id'))}, "
            f"{_v(row.get('partida_id'))}, "
            f"{_v(row.get('donacion'), quote=True)}, "
            f"{_v(row.get('item_id'))}, "
            f"{_v(row.get('unidad_medida_id'))}, "
            f"{_v(row.get('cantidad'))}, "
            f"{_v(row.get('costo'))}, "
            f"{_v(row.get('total'))}, "
            f"{_v(row.get('created_at'), quote=True)}, "
            f"{_v(row.get('updated_at'), quote=True)}"
            ");"
        )
        lineas.append(insert)

    lineas.append("")

    # ---- 2. UPDATE ingresos.total (suma de totales por ingreso_id) ---
    lineas.append("-- ---- UPDATE ingresos.total ----")
    totales_grouped = (
        df.groupby("ingreso_id")["total"]
        .sum()
        .reset_index()
        .rename(columns={"total": "suma_total"})
    )
    for _, row in totales_grouped.iterrows():
        lineas.append(
            f"UPDATE `ingresos` SET `total` = {float(row['suma_total']):.2f} "
            f"WHERE `id` = {int(row['ingreso_id'])};"
        )

    lineas.append("")

    # ---- 3. UPDATE ingresos.etapa_ingreso ---------------------------
    lineas.append("-- ---- UPDATE ingresos.etapa_ingreso ----")
    # Para cada ingreso_id tomamos la etapa (si hay mix tomamos la primera)
    etapa_grouped = (
        etapas_df.groupby("ingreso_id")["_etapa"]
        .first()
        .reset_index()
        .rename(columns={"_etapa": "etapa"})
    )
    for _, row in etapa_grouped.iterrows():
        lineas.append(
            f"UPDATE `ingresos` SET `etapa_ingreso` = '{row['etapa']}' "
            f"WHERE `id` = {int(row['ingreso_id'])};"
        )

    lineas.append("")
    lineas.append("SET FOREIGN_KEY_CHECKS = 1;")
    lineas.append("")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lineas))

    print(
        f"[ingreso_detalles_migration] SQL generado: {output_path} "
        f"({len(df)} INSERTs, {len(totales_grouped)} UPDATEs total, "
        f"{len(etapa_grouped)} UPDATEs etapa)"
    )
    return output_path
