"""
verificacion/check_valores.py
==============================
Verifica que el precio unitario (valor) sea consistente en cada fila
de las hojas detalle del Excel.

Columnas verificadas (las 4 columnas "valor"):
  - SALDO_AL_01_DE_ENERO_DE_2025_valor
  - INGRESO_ALMACENES_VALOR
  - SALIDA_ALMACENES_VALOR
  - SALDO_AL_31_DE_DICIEMBRE_DE_2025_valor

Regla: si una fila tiene 2 o más de estas columnas con valor no nulo,
todos esos valores deben ser iguales entre sí.

Columnas nulas/vacías → se ignoran sin reportar error.
"""

import logging
import os
import sys

import pandas as pd

# ---- Asegurar que el root del proyecto esté en el path ----
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from excel_loader import load_dfs_limpios, DETALLES

# ---- Logger dedicado a esta verificación ----
_LOG_DIR = os.path.join(_ROOT, "output")
_LOG_FILE = os.path.join(_LOG_DIR, "check_valores.log")

os.makedirs(_LOG_DIR, exist_ok=True)

logger = logging.getLogger("check_valores")
logger.setLevel(logging.DEBUG)

_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

_fh = logging.FileHandler(_LOG_FILE, encoding="utf-8", mode="w")
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(_fmt)
logger.addHandler(_fh)

_ch = logging.StreamHandler()
_ch.setLevel(logging.INFO)
_ch.setFormatter(_fmt)
logger.addHandler(_ch)

# ---- Columnas a verificar ----
COLS_VALOR = [
    "SALDO_AL_01_DE_ENERO_DE_2025_valor",
    "INGRESO_ALMACENES_VALOR",
    "SALIDA_ALMACENES_VALOR",
    "SALDO_AL_31_DE_DICIEMBRE_DE_2025_valor",
]

_TOLERANCIA = 1e-6


def _es_nulo(val) -> bool:
    """Retorna True si el valor es nulo, vacío o cero no significativo."""
    if val is None:
        return True
    try:
        if pd.isna(val):
            return True
    except Exception:
        pass
    s = str(val).strip()
    return s in ("", "None", "nan", "NaN")


def _to_float(val) -> float | None:
    """Convierte a float; retorna None si no es posible."""
    try:
        return float(str(val).replace(",", ".").strip())
    except (ValueError, TypeError):
        return None


def verificar_hoja(nombre_hoja: str, df: pd.DataFrame) -> dict:
    """
    Verifica la consistencia de las columnas valor en una hoja detalle.

    Retorna un dict con estadísticas:
        filas_ok, filas_alerta, filas_sin_datos
    """
    cols_presentes = [c for c in COLS_VALOR if c in df.columns]

    if not cols_presentes:
        logger.warning(
            f"[{nombre_hoja}] Ninguna columna valor encontrada. "
            f"Columnas disponibles: {list(df.columns)}"
        )
        return {"filas_ok": 0, "filas_alerta": 0, "filas_sin_datos": len(df)}

    cols_ausentes = [c for c in COLS_VALOR if c not in df.columns]
    if cols_ausentes:
        logger.warning(
            f"[{nombre_hoja}] Columnas no encontradas (se omiten): {cols_ausentes}"
        )

    logger.info(f"[{nombre_hoja}] Iniciando verificación — {len(df)} filas, columnas: {cols_presentes}")

    filas_ok = 0
    filas_alerta = 0
    filas_sin_datos = 0

    for idx, row in df.iterrows():
        valores_no_nulos: dict[str, float] = {}

        for col in cols_presentes:
            val = row.get(col)
            if not _es_nulo(val):
                num = _to_float(val)
                if num is not None:
                    valores_no_nulos[col] = num

        if len(valores_no_nulos) == 0:
            filas_sin_datos += 1
            logger.debug(
                f"[{nombre_hoja}] fila {idx} — sin datos en columnas valor (todas nulas)"
            )
            continue

        if len(valores_no_nulos) == 1:
            filas_ok += 1
            col_unica, val_unica = next(iter(valores_no_nulos.items()))
            logger.debug(
                f"[{nombre_hoja}] fila {idx} — OK (solo una columna con dato: "
                f"{col_unica}={val_unica})"
            )
            continue

        # ---- Verificar que todos los valores no nulos sean iguales ----
        valores_lista = list(valores_no_nulos.values())
        referencia = valores_lista[0]
        todos_iguales = all(
            abs(v - referencia) <= _TOLERANCIA for v in valores_lista[1:]
        )

        detalle = ", ".join(f"{c}={v}" for c, v in valores_no_nulos.items())

        if todos_iguales:
            filas_ok += 1
            logger.info(
                f"[{nombre_hoja}] fila {idx} — SUCCESS [{detalle}]"
            )
        else:
            filas_alerta += 1
            logger.warning(
                f"[{nombre_hoja}] fila {idx} — ALERTA valores no coinciden: [{detalle}]"
            )

    return {
        "filas_ok": filas_ok,
        "filas_alerta": filas_alerta,
        "filas_sin_datos": filas_sin_datos,
    }


def run(dfs_limpios: dict | None = None) -> None:
    """
    Ejecuta la verificación en todas las hojas detalle.

    Args:
        dfs_limpios: dict ya cargado (opcional; si None lo carga internamente)
    """
    if dfs_limpios is None:
        dfs_limpios = load_dfs_limpios()

    hojas_detalle = {k: v for k, v in dfs_limpios.items() if k in DETALLES}

    logger.info("=" * 60)
    logger.info("VERIFICACIÓN: consistencia de columnas valor")
    logger.info(f"Hojas detalle a verificar: {list(hojas_detalle.keys())}")
    logger.info("=" * 60)

    resumen_global = {"filas_ok": 0, "filas_alerta": 0, "filas_sin_datos": 0}

    for nombre_hoja, df in hojas_detalle.items():
        logger.info("-" * 60)
        stats = verificar_hoja(nombre_hoja, df)

        logger.info(
            f"[{nombre_hoja}] RESUMEN → "
            f"OK={stats['filas_ok']} | "
            f"ALERTA={stats['filas_alerta']} | "
            f"SIN DATOS={stats['filas_sin_datos']}"
        )

        for k in resumen_global:
            resumen_global[k] += stats[k]

    logger.info("=" * 60)
    logger.info(
        f"RESUMEN GLOBAL → "
        f"OK={resumen_global['filas_ok']} | "
        f"ALERTA={resumen_global['filas_alerta']} | "
        f"SIN DATOS={resumen_global['filas_sin_datos']}"
    )
    logger.info(f"Log detallado guardado en: {_LOG_FILE}")
    logger.info("=" * 60)


if __name__ == "__main__":
    run()
