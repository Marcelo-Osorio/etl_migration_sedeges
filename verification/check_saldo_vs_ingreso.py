"""
verificacion/check_saldo_vs_ingreso.py
========================================
Detecta posibles incoherencias en hojas detalle donde una misma fila
tiene datos > 0 en el grupo SALDO INICIAL y también en el grupo INGRESOS.

Grupos verificados:
  GRUPO A — Saldo inicial:
    - SALDO_AL_01_DE_ENERO_DE_2025_CANT
    - SALDO_AL_01_DE_ENERO_DE_2025_valor
    - SALDO_AL_01_DE_ENERO_DE_2025_TOTAL Bs.

  GRUPO B — Ingresos almacenes:
    - INGRESO_ALMACENES_CANT
    - INGRESO_ALMACENES_VALOR
    - INGRESO_ALMACENES_TOTAL Bs.

Regla: si al menos 1 columna de GRUPO A tiene dato > 0
       Y al menos 1 columna de GRUPO B tiene dato > 0
       → ALERTA posible incoherencia (se muestra la fila completa).

Solo se emiten logs de ALERTA; las filas sin incoherencia no se registran.
"""

import logging
import os
import sys

import pandas as pd

# ---- Root del proyecto en el path ----
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from excel_loader import load_dfs_limpios, DETALLES

# ---- Logger dedicado ----
_LOG_DIR = os.path.join(_ROOT, "output")
_LOG_FILE = os.path.join(_LOG_DIR, "check_saldo_vs_ingreso.log")

os.makedirs(_LOG_DIR, exist_ok=True)

logger = logging.getLogger("check_saldo_vs_ingreso")
logger.setLevel(logging.DEBUG)

_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

_fh = logging.FileHandler(_LOG_FILE, encoding="utf-8", mode="w")
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(_fmt)
logger.addHandler(_fh)

_ch = logging.StreamHandler()
_ch.setLevel(logging.WARNING)
_ch.setFormatter(_fmt)
logger.addHandler(_ch)

# ---- Definición de grupos ----
GRUPO_A = [
    "SALDO_AL_01_DE_ENERO_DE_2025_CANT",
    "SALDO_AL_01_DE_ENERO_DE_2025_valor",
    "SALDO_AL_01_DE_ENERO_DE_2025_TOTAL Bs.",
]

GRUPO_B = [
    "INGRESO_ALMACENES_CANT",
    "INGRESO_ALMACENES_VALOR",
    "INGRESO_ALMACENES_TOTAL Bs.",
]

ALL_COLS = GRUPO_A + GRUPO_B


def _tiene_dato(val) -> bool:
    """Retorna True si el valor es numérico y mayor a cero."""
    if val is None:
        return False
    try:
        if pd.isna(val):
            return False
    except Exception:
        pass
    try:
        return float(str(val).replace(",", ".").strip()) > 0
    except (ValueError, TypeError):
        return False


def _fila_a_texto(row: pd.Series, cols: list[str]) -> str:
    """Formatea solo las columnas relevantes de una fila como texto legible."""
    partes = []
    for col in cols:
        if col in row.index:
            val = row[col]
            es_nulo = pd.isna(val) if not isinstance(val, str) else val.strip() == ""
            partes.append(f"{col}={'(nulo)' if es_nulo else val}")
    return " | ".join(partes)


def verificar_hoja(nombre_hoja: str, df: pd.DataFrame) -> int:
    """
    Verifica una hoja detalle. Retorna el número de alertas encontradas.
    """
    cols_a = [c for c in GRUPO_A if c in df.columns]
    cols_b = [c for c in GRUPO_B if c in df.columns]

    if not cols_a or not cols_b:
        logger.debug(
            f"[{nombre_hoja}] Sin columnas suficientes para verificar "
            f"(GRUPO_A presentes={cols_a}, GRUPO_B presentes={cols_b}). Se omite."
        )
        return 0

    alertas = 0

    for idx, row in df.iterrows():
        tiene_a = any(_tiene_dato(row.get(c)) for c in cols_a)
        tiene_b = any(_tiene_dato(row.get(c)) for c in cols_b)

        if tiene_a and tiene_b:
            alertas += 1
            fila_txt = _fila_a_texto(row, ALL_COLS)
            logger.warning(
                f"[{nombre_hoja}] fila {idx} — POSIBLE INCOHERENCIA: "
                f"datos en saldo inicial Y en ingresos simultáneamente | {fila_txt}"
            )

    return alertas


def run(dfs_limpios: dict | None = None) -> None:
    """
    Ejecuta la verificación en todas las hojas detalle.

    Args:
        dfs_limpios: dict ya cargado (opcional; si None lo carga internamente)
    """
    if dfs_limpios is None:
        dfs_limpios = load_dfs_limpios()

    hojas_detalle = {k: v for k, v in dfs_limpios.items() if k in DETALLES}

    logger.debug("=" * 60)
    logger.debug("VERIFICACIÓN: saldo inicial vs ingresos almacenes")
    logger.debug(f"Hojas detalle: {list(hojas_detalle.keys())}")
    logger.debug("=" * 60)

    total_alertas = 0
    total_filas = 0

    for nombre_hoja, df in hojas_detalle.items():
        logger.debug(f"[{nombre_hoja}] Verificando {len(df)} filas...")
        n = verificar_hoja(nombre_hoja, df)
        total_alertas += n
        total_filas += len(df)
        logger.debug(f"[{nombre_hoja}] Alertas encontradas: {n}")

    logger.debug("=" * 60)
    logger.debug(
        f"RESUMEN: {total_filas} filas revisadas, "
        f"{total_alertas} alertas de incoherencia"
    )
    logger.debug(f"Log completo en: {_LOG_FILE}")
    logger.debug("=" * 60)

    # Siempre mostrar el resumen final en consola
    print(f"\n{'=' * 50}")
    print(f"Filas revisadas : {total_filas}")
    print(f"Alertas emitidas: {total_alertas}")
    print(f"Log guardado en : {_LOG_FILE}")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    run()
