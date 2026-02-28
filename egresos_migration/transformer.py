"""
egresos_migration.transformer
=============================

Aquí está la parte principal de la lógica de negocio de los egresos.

Recibe:
  - df_ingreso_detalles: filas reales de la tabla ingreso_detalles (id > 7)
  - df_limpio: todas las SALIDAS del Excel (ya unificadas por almacén)
  - item_id_to_nombre: diccionario {item_id: nombre} de catalogo_items

Y devuelve un DataFrame con la forma exacta de la tabla `egresos`,
aplicando las validaciones que definiste:
  - (ingreso_id, ingreso_detalle_id) debe ser único
  - DESCRIPCION (Excel) debe coincidir con catalogo_items.nombre
"""

from datetime import datetime

import pandas as pd

from utils.logger import get_logger
import unicodedata


logger = get_logger()

_COL_CANT = "SALIDA_ALMACENES_CANT"
_COL_VALOR = "SALIDA_ALMACENES_VALOR"
_COL_TOTAL = "SALIDA_ALMACENES_TOTAL Bs."


def _to_num(val, default=0):
    """
    Convierte valores del Excel (strings, números, NaN) a número flotante.
    Si no se puede convertir, devuelve `default`.
    """
    try:
        if pd.isna(val):
            return default
    except Exception:
        pass
    try:
        return float(str(val).replace(",", ".").strip())
    except (ValueError, TypeError):
        return default


def _norm(s) -> str:
    """
    Normaliza texto para comparar:
      - convierte a string
      - trim
      - minúsculas
      - elimina acentos/diacríticos (NFD)
      - normaliza espacios
    """
    if s is None:
        return ""
    try:
        if pd.isna(s):
            return ""
    except Exception:
        pass

    text = str(s).strip().lower()

    # 1) Separar letras de sus diacríticos (acentos)
    text = unicodedata.normalize("NFD", text)
    # 2) Quitar los diacríticos (categoría Mn)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    # 3) Normalizar espacios (opcional, pero útil)
    text = " ".join(text.split())

    return text


def build_egresos_df(
    df_ingreso_detalles: pd.DataFrame,
    df_limpio: pd.DataFrame,
    item_id_to_nombre: dict[int, str],
) -> pd.DataFrame:
    """
    Construye el DataFrame final para la tabla `egresos`.

    Resumen rápido:
      1. Por cada fila de `ingreso_detalles` buscamos su fila gemela en el Excel en la misma posición
         usando:
             - DESCRIPCION normalizada == nombre del item (catalogo_items)
      2. Copiamos:
             SALIDA_ALMACENES_CANT  → cantidad
             SALIDA_ALMACENES_VALOR → costo
             SALIDA_ALMACENES_TOTAL Bs. → total
      3. Forzamos estas reglas:
           - (ingreso_id, ingreso_detalle_id) no se puede repetir
           - si no hay coincidencia en Excel → error y se detiene
           - si hay más de una coincidencia → error y se detiene
      4. Rellenamos destino_id = NULL, editable = 1 y fechas actuales.
    """

    ahora = datetime.now()
    fecha_registro = ahora.strftime("%Y-%m-%d")
    created_at = ahora.strftime("%Y-%m-%d %H:%M:%S")
    updated_at = ahora.strftime("%Y-%m-%d %H:%M:%S")

    # --- Validación: ambos DF deben tener el mismo número de filas ---
    n_det = len(df_ingreso_detalles)
    n_xls = len(df_limpio)
    if n_det != n_xls:
        msg = (
            "No se puede construir egresos por posición porque el número de filas no coincide.\n"
            f"- df_ingreso_detalles: {n_det} filas\n"
            f"- df_limpio: {n_xls} filas\n"
            "Solución: asegúrate de que ambos DataFrames estén alineados 1 a 1 y en el mismo orden."
        )
        logger.error(msg)
        raise ValueError(msg)

    # --- Preparamos df_limpio ---
    df_limpio = df_limpio.copy()

    if "DESCRIPCION" not in df_limpio.columns:
        msg = "df_limpio no tiene la columna requerida 'DESCRIPCION'."
        logger.error(msg)
        raise ValueError(msg)

    df_limpio["_desc_norm"] = df_limpio["DESCRIPCION"].map(_norm)

    # --- Validación de unicidad de pares (ingreso_id, ingreso_detalle_id) ---
    seen_pairs: set[tuple[int, int]] = set()

    rows_out = []

    # Recorremos por índice posicional (i)
    for i, (_, det) in enumerate(df_ingreso_detalles.iterrows()):
        # IDs principales
        ingreso_id = int(det["ingreso_id"])
        ingreso_detalle_id = int(det["id"])
        pair = (ingreso_id, ingreso_detalle_id)

        if pair in seen_pairs:
            msg = (
                f"Duplicado detectado: (ingreso_id, ingreso_detalle_id)=({ingreso_id}, {ingreso_detalle_id}).\n"
                f"- pos i={i}\n"
                "Cancelando ejecución."
            )
            logger.error(msg)
            raise ValueError(msg)
        seen_pairs.add(pair)

        # Campos del detalle
        almacen_id = int(det["almacen_id"]) if pd.notna(det["almacen_id"]) else None

        partida_id = det["partida_id"] if pd.notna(det.get("partida_id", None)) else None
        if partida_id is not None:
            partida_id = int(partida_id)

        item_id = int(det["item_id"])
        nombre_item = item_id_to_nombre.get(item_id, "")
        nombre_norm = _norm(nombre_item)

        # 1) Tomamos la fila EXACTA del Excel por posición i
        row_excel = df_limpio.iloc[i]

        # 2) Validamos descripción
        desc_excel_raw = row_excel.get("DESCRIPCION", "")
        desc_excel_norm = row_excel.get("_desc_norm", "")

        if desc_excel_norm != nombre_norm:
            msg = (
                "Validación DESCRIPCION fallida por posición.\n"
                f"- pos i={i}\n"
                f"- ingreso_id={ingreso_id}, ingreso_detalle_id={ingreso_detalle_id}, item_id={item_id}\n"
                f"- Excel.DESCRIPCION='{desc_excel_raw}' (norm='{desc_excel_norm}')\n"
                f"- catalogo_items.nombre='{nombre_item}' (norm='{nombre_norm}')\n"
            )
            logger.error(msg)
            raise ValueError(msg)

        # 3) Extraer cantidad / costo / total de ESA MISMA fila
        cantidad = int(_to_num(row_excel.get(_COL_CANT), 0))
        costo = _to_num(row_excel.get(_COL_VALOR), 0)
        total = _to_num(row_excel.get(_COL_TOTAL), 0)

        # 4) Armar salida
        rows_out.append(
            {
                "ingreso_id": ingreso_id,
                "ingreso_detalle_id": ingreso_detalle_id,
                "almacen_id": almacen_id,
                "partida_id": partida_id,
                "item_id": item_id,
                "destino_id": None,
                "cantidad": cantidad,
                "costo": costo,
                "total": total,
                "fecha_registro": fecha_registro,
                "editable": 1,
                "created_at": created_at,
                "updated_at": updated_at,
            }
        )

    df_out = pd.DataFrame(rows_out)
    logger.info(f"DataFrame egresos construido: {len(df_out)} filas")
    return df_out
