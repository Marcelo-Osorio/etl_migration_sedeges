"""
items.migration.transformer
===========================
Aplica las transformaciones sobre el DataFrame consolidado de ítems detalle
y construye las columnas finales para la tabla `catalogo_items`:

    nombre      → DESCRIPCION en minúsculas (str.lower + strip)
    grupo       → Determinado por reglas de negocio (ver _asignar_grupo)
    abreviatura → Parte alfabética del CODIGO (ej. "ALM-10" → "ALM")
"""

import re
import pandas as pd


# ------------------------------------------------------------------ #
# REGLAS PARA EL CAMPO  `grupo`                                      #
# ------------------------------------------------------------------ #
# Prioridad (de mayor a menor):
#   1. FARMACIA  → usa el valor de la columna GRUPO del propio df
#   2. SERVICIO  → si UNIDAD == "SERVICIO"  OR  descripcion empieza con "registro"
#   3. TRAMITE   → si descripcion empieza con "cierre"
#   4. PRODUCTO  → valor por defecto


def _asignar_grupo(row: pd.Series) -> str:
    """
    Determina el valor de `grupo` para una fila según las reglas de negocio.

    Args:
        row: fila del DataFrame que contiene hoja_origen, DESCRIPCION,
             UNIDAD y GRUPO

    Returns:
        str con el grupo asignado
    """
    # Normalizar valores para comparación segura
    hoja: str = str(row.get("hoja_origen", "")).strip().upper()
    descripcion: str = str(row.get("DESCRIPCION", "")).strip().lower()
    unidad: str = str(row.get("UNIDAD", "")).strip().upper()
    grupo_farmacia = row.get("GRUPO")

    # --- Regla 1: FARMACIA usa su propia columna GRUPO ---
    if hoja == "FARMACIA":
        if pd.notna(grupo_farmacia) and str(grupo_farmacia).strip():
            return str(grupo_farmacia).strip().lower()
        # Si por algún motivo GRUPO está vacío en FARMACIA, caer a producto
        return "producto"

    # --- Regla 2: SERVICIO ---
    # Condición A: la columna UNIDAD dice "SERVICIO"
    if unidad == "SERVICIO":
        return "servicio"

    # Condición B: la descripción empieza con "registro"
    if re.match(r"^registro\b", descripcion, flags=re.IGNORECASE):
        return "servicio"

    # --- Regla 3: TRAMITE ---
    # La descripción empieza con "cierre"
    if re.match(r"^cierre\b", descripcion, flags=re.IGNORECASE):
        return "tramite"

    # --- Regla 4: Por defecto es producto ---
    return "producto"


# ------------------------------------------------------------------ #
# REGLA PARA EL CAMPO  `abreviatura`                                 #
# ------------------------------------------------------------------ #
# Los códigos tienen el formato  LETRAS-NÚMERO  (ej. "P-1", "LIM-4", "UEO-10")
# Solo queremos la parte alphabética: "P", "LIM", "UEO"


def _extraer_abreviatura(codigo) -> str | None:
    """
    Extrae la parte puramente alfabética antes del guion del código.

    Ejemplos:
        "P-1"    → "P"
        "LIM-4"  → "LIM"
        "UEO-10" → "UEO"
        "ALM-10" → "ALM"
        nan / ""  → None
    """
    if pd.isna(codigo) or str(codigo).strip() == "":
        return None

    # Busca uno o más caracteres alfabéticos al inicio, opcionalmente seguidos de guion
    match = re.match(r"^([A-Za-záéíóúÁÉÍÓÚñÑ]+)", str(codigo).strip())
    if match:
        return match.group(1).upper()  # devolvemos en mayúsculas (ej. "LIM")
    return None


# ------------------------------------------------------------------ #
# FUNCIÓN PRINCIPAL                                                   #
# ------------------------------------------------------------------ #


def transform_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recibe el DataFrame crudo del extractor y devuelve uno transformado con
    las columnas listas para insertar en `catalogo_items`.

    Columnas de salida:
        nombre       str  → descripción en minúsculas
        grupo        str  → producto | servicio | tramite | <grupo farmacia>
        abreviatura  str  → parte alfabética del código original

    Args:
        df: DataFrame de extract_items_from_detalle()

    Returns:
        DataFrame con columnas [nombre, grupo, abreviatura]
    """
    resultado = pd.DataFrame()

    # --- nombre: DESCRIPCION en minúsculas y sin espacios extremos ---
    resultado["nombre"] = df["DESCRIPCION"].astype(str).str.strip().str.lower()

    # --- grupo: aplicar reglas fila por fila ---
    resultado["grupo"] = df.apply(_asignar_grupo, axis=1)

    # --- abreviatura: parte alfabética del CODIGO ---
    resultado["abreviatura"] = df["CODIGO"].apply(_extraer_abreviatura)

    print(f"[transformer] DataFrame transformado: {len(resultado)} filas")
    print(
        f"[transformer] Distribución de grupos:\n{resultado['grupo'].value_counts().to_string()}"
    )

    return resultado
