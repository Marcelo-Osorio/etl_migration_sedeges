import os

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv

from rules import clean_detalle, clean_farmacia, clean_contable
from excel_export import export_book_to_excel
from items_migration import build_catalogo_items_df
from items_migration.exporter_sql import export_items_to_sql

load_dotenv()

DB_URL = (
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '3306')}"
    f"/{os.getenv('DB_NAME')}"
)
engine = create_engine(DB_URL)

archivo_url = "data/exel_sedeges.xlsx"

range_columns_details = {
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

range_columns_contable = {
    "ANEXO-1B": range(0, 6),
    "ANEXO-1C": range(0, 8),
    "DONACIONES": range(0, 5),
}

DETALLES = set(range_columns_details.keys())
CONTABLES = set(range_columns_contable.keys())


def wipe_sheet(libro: dict, name_sheet: str) -> pd.DataFrame:
    df = libro[name_sheet]

    # Recortar columnas según el rango
    if name_sheet in range_columns_details:
        df = df.iloc[:, range_columns_details[name_sheet]]
    if name_sheet in range_columns_contable:
        df = df.iloc[:, range_columns_contable[name_sheet]]

    if name_sheet == "FARMACIA":
        return clean_farmacia(df)

    if name_sheet in CONTABLES:
        return clean_contable(df)

    # Por defecto: reglas de hoja detalle
    return clean_detalle(df)


def procesar_etl():
    libro = pd.read_excel(archivo_url, sheet_name=None)

    # Transformacion de TODO el libro a DataFrames limpios
    dfs_limpios = {}
    for sheet_name in libro.keys():
        if sheet_name in range_columns_details or sheet_name in range_columns_contable:
            dfs_limpios[sheet_name] = wipe_sheet(libro, sheet_name)

    print("Hojas procesadas:", list(dfs_limpios.keys()))

    # Migracion de items solo operado por hojas detalle
    df_items = build_catalogo_items_df(dfs_limpios)

    print(
        f"\nDataFrame de items listo: {df_items.shape[0]} filas x {df_items.shape[1]} columnas"
    )
    print(df_items.head(5).to_string())

    # Exportar a archivo SQL en output/catalogo_items.sql
    ruta_sql = export_items_to_sql(df_items, filename="catalogo_items.sql")
    print(f"\nSQL generado: {ruta_sql}")


if __name__ == "__main__":
    procesar_etl()
