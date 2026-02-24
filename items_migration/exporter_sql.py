"""
items.migration.exporter_sql
=============================
Exporta el DataFrame de catálogo de ítems a:

  A) Un archivo SQL con sentencias INSERT INTO para la tabla `catalogo_items`
     → output/catalogo_items.sql

  B) (Opcional) Directamente a la base de datos MySQL usando SQLAlchemy.
     La conexión se construye leyendo las variables de entorno definidas en .env

Campos generados automáticamente:
    fecha_registro  → fecha actual  SIN hora  (DATE)
    created_at      → datetime actual CON hora (TIMESTAMP)
    updated_at      → datetime actual CON hora (TIMESTAMP)
"""

import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Cargar variables de entorno desde .env en la raíz del proyecto
load_dotenv()

# ------------------------------------------------------------------ #
#  Carpeta de salida                                                  #
# ------------------------------------------------------------------ #
_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")


def _get_engine():
    """
    Construye el engine de SQLAlchemy usando las variables de entorno.
    Lanza un error claro si alguna variable falta.
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "3306")
    name = os.getenv("DB_NAME")

    if not all([user, password, name]):
        raise EnvironmentError(
            "Faltan variables de entorno para la DB. "
            "Verifica que .env contiene: DB_USER, DB_PASSWORD, DB_NAME"
        )

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}"
    return create_engine(url)


def _escape_sql_string(valor) -> str:
    """
    Escapa una cadena de texto para uso seguro dentro de un INSERT SQL.
    Reemplaza comillas simples por dos comillas simples (estándar SQL).

    Args:
        valor: valor a escapar (puede ser str, None, NaN)

    Returns:
        'valor_escapado' o NULL si es nulo/vacío
    """
    if pd.isna(valor) or str(valor).strip() == "" or str(valor) == "None":
        return "NULL"
    # Escapar comillas simples duplicándolas
    escapado = str(valor).replace("'", "''")
    return f"'{escapado}'"


def export_items_to_sql(df: pd.DataFrame, filename: str = "catalogo_items.sql") -> str:
    """
    Genera un archivo .sql con sentencias INSERT INTO para la tabla
    `catalogo_items`, incluyendo los campos de auditoría con la fecha actual.

    Args:
        df:       DataFrame con columnas [nombre, grupo, abreviatura]
        filename: nombre del archivo SQL a generar en la carpeta output/

    Returns:
        Ruta absoluta del archivo SQL generado
    """
    # Asegurarse de que la carpeta output/ existe
    os.makedirs(_OUTPUT_DIR, exist_ok=True)

    output_path = os.path.join(_OUTPUT_DIR, filename)

    # Timestamps actuales
    ahora = datetime.now()
    fecha_registro = ahora.strftime("%Y-%m-%d")  # solo fecha, sin hora
    created_at = ahora.strftime("%Y-%m-%d %H:%M:%S")  # fecha + hora
    updated_at = ahora.strftime("%Y-%m-%d %H:%M:%S")  # fecha + hora

    lineas = []

    # Cabecera del archivo SQL
    lineas.append("-- ============================================================")
    lineas.append("-- Migración: catalogo_items")
    lineas.append(f"-- Generado el: {ahora.strftime('%Y-%m-%d %H:%M:%S')}")
    lineas.append(f"-- Total de registros: {len(df)}")
    lineas.append("-- ============================================================")
    lineas.append("")
    lineas.append("SET NAMES utf8mb4;")
    lineas.append("SET FOREIGN_KEY_CHECKS = 0;")
    lineas.append("")

    # Una sentencia INSERT por fila para mayor legibilidad y seguridad
    for _, row in df.iterrows():
        nombre = _escape_sql_string(row.get("nombre"))
        grupo = _escape_sql_string(row.get("grupo"))
        abreviatura = _escape_sql_string(row.get("abreviatura"))

        insert = (
            f"INSERT INTO `catalogo_items` "
            f"(`nombre`, `grupo`, `abreviatura`, `fecha_registro`, `created_at`, `updated_at`) "
            f"VALUES ("
            f"{nombre}, "
            f"{grupo}, "
            f"{abreviatura}, "
            f"'{fecha_registro}', "
            f"'{created_at}', "
            f"'{updated_at}'"
            f");"
        )
        lineas.append(insert)

    lineas.append("")
    lineas.append("SET FOREIGN_KEY_CHECKS = 1;")
    lineas.append("")

    # Escribir el archivo con encoding UTF-8
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lineas))

    print(f"[exporter_sql] SQL generado en: {output_path} ({len(df)} registros)")
    return output_path


def export_items_to_db(df: pd.DataFrame, tabla: str = "catalogo_items"):
    """
    Inserta el DataFrame directamente en la base de datos usando SQLAlchemy.
    La conexión se obtiene de las variables de entorno en .env

    ⚠️ Usa if_exists='append' para NO borrar datos existentes.
       Si quieres reemplazar, cambia a if_exists='replace'.

    Args:
        df:    DataFrame con columnas [nombre, grupo, abreviatura]
        tabla: nombre de la tabla destino en MySQL
    """
    engine = _get_engine()

    # Agregar columnas de auditoría al DataFrame antes de insertar
    ahora = datetime.now()
    df_db = df.copy()
    df_db["fecha_registro"] = ahora.date()  # solo fecha
    df_db["created_at"] = ahora  # datetime completo
    df_db["updated_at"] = ahora  # datetime completo

    # Insertar usando pandas to_sql (append = no borra filas existentes)
    df_db.to_sql(
        name=tabla,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",  # batch insert para mejor rendimiento
        chunksize=500,  # insertar de a 500 filas
    )

    print(f"[exporter_sql] {len(df_db)} registros insertados en `{tabla}`")
