"""
excel_loader.py
===============
Carga y limpieza centralizada de todas las hojas del Excel SEDEGES.

Antes esta lógica vivía en run_catalogo_items.py; ahora es un módulo
independiente para que cualquier script de migración lo reutilice.

Uso:
    from excel_loader import load_dfs_limpios
    dfs = load_dfs_limpios()
"""

import pandas as pd

from rules import clean_detalle, clean_farmacia, clean_contable

ARCHIVO_URL = "data/exel_sedeges.xlsx"

RANGE_COLUMNS_DETAILS = {
    "ANEXO-1A": range(0, 17),
    "PCVH MUJER": range(0, 17),
    "PAIPI": range(0, 17),
    "CEPAT": range(0, 17),
    "PDP MUJERES IDH": range(0, 17),
    "PAAMCTI PROVINCIAL": range(0, 17),
    "PPAER PENAL": range(0, 17),
    "POAR PENAL": range(0, 17),
    "PMA NIÑNIÑO ADOL": range(0, 17),
    "PCEMTRATA Y TRAFICO": range(0, 17),
    "BECAS": range(0, 17),
    "REFRIGERIOS": range(0, 17),
    "VIVERES FRESCOS": range(0, 17),
    "FONDOS EN AVANCE": range(0, 17),
    "FARMACIA": range(0, 17),
}

RANGE_COLUMNS_CONTABLE = {
    "ANEXO-1B": range(0, 6),
    "ANEXO-1C": range(0, 8),
    "DONACIONES": range(0, 5),
}

DETALLES = set(RANGE_COLUMNS_DETAILS.keys())
CONTABLES = set(RANGE_COLUMNS_CONTABLE.keys())


def _wipe_sheet(libro: dict, name_sheet: str) -> pd.DataFrame:
    df = libro[name_sheet]
    if name_sheet in RANGE_COLUMNS_DETAILS:
        df = df.iloc[:, RANGE_COLUMNS_DETAILS[name_sheet]]
    if name_sheet in RANGE_COLUMNS_CONTABLE:
        df = df.iloc[:, RANGE_COLUMNS_CONTABLE[name_sheet]]
    if name_sheet == "FARMACIA":
        return clean_farmacia(df)
    if name_sheet in CONTABLES:
        return clean_contable(df)
    return clean_detalle(df)


def load_dfs_limpios() -> dict:
    """Carga y limpia todas las hojas del Excel."""
    libro = pd.read_excel(ARCHIVO_URL, sheet_name=None)
    dfs_limpios = {}
    for sheet_name in libro.keys():
        if sheet_name in RANGE_COLUMNS_DETAILS or sheet_name in RANGE_COLUMNS_CONTABLE:
            dfs_limpios[sheet_name] = _wipe_sheet(libro, sheet_name)
    print("Hojas procesadas:", list(dfs_limpios.keys()))
    return dfs_limpios
