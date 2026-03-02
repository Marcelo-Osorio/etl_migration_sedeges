"""
donaciones_migration.validator
===============================
Validaciones contra la BD previas a la construcción de los DataFrames
de DONACIONES.

Verifica:
  1. almacen_id = 25 → nombre = "DONACION SIN ALMACEN"
  2. user_id = 1    → usuario = "admin"
  3. Cada PARTIDA del Excel tiene exactamente 1 match en tabla partidas
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.logger import get_logger

logger = get_logger()

ALMACEN_ID_DONACION = 25
USER_ID_ADMIN = 1


def validate_almacen(engine: Engine) -> None:
    """Verifica que almacen_id=25 corresponda a 'DONACION SIN ALMACEN'."""
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT nombre FROM almacens WHERE id = :id"),
            {"id": ALMACEN_ID_DONACION},
        ).fetchone()

    if row is None:
        msg = (
            f"No existe el almacen con id={ALMACEN_ID_DONACION} en la tabla almacens. "
            "No se puede continuar con la migración de DONACIONES."
        )
        logger.error(msg)
        raise RuntimeError(msg)

    nombre = str(row[0]).strip().upper()
    if nombre != "DONACION SIN ALMACEN":
        msg = (
            f"El almacen con id={ALMACEN_ID_DONACION} tiene nombre='{row[0]}', "
            "pero se esperaba 'DONACION SIN ALMACEN'. "
            "No se puede continuar."
        )
        logger.error(msg)
        raise RuntimeError(msg)

    logger.info(
        f"Almacen id={ALMACEN_ID_DONACION} verificado: '{row[0]}'"
    )


def validate_user(engine: Engine) -> None:
    """Verifica que user_id=1 corresponda al usuario 'admin'."""
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT usuario FROM users WHERE id = :id"),
            {"id": USER_ID_ADMIN},
        ).fetchone()

    if row is None:
        msg = (
            f"No existe el usuario con id={USER_ID_ADMIN} en la tabla users. "
            "No se puede continuar con la migración de DONACIONES."
        )
        logger.error(msg)
        raise RuntimeError(msg)

    usuario = str(row[0]).strip().lower()
    if usuario != "admin":
        msg = (
            f"El usuario con id={USER_ID_ADMIN} tiene usuario='{row[0]}', "
            "pero se esperaba 'admin'. No se puede continuar."
        )
        logger.error(msg)
        raise RuntimeError(msg)

    logger.info(f"Usuario id={USER_ID_ADMIN} verificado: '{row[0]}'")


def resolve_partida_ids(
    df_donaciones: pd.DataFrame, engine: Engine
) -> list[int]:
    """
    Para cada fila de DONACIONES, busca el nro_partida en la tabla
    partidas y devuelve una lista de partida_id (mismo orden que df).

    Lanza excepción si alguna partida no se encuentra o tiene duplicados.
    """
    partida_ids: list[int] = []
    cache: dict[str, int] = {}

    with engine.connect() as conn:
        for idx, row in df_donaciones.iterrows():
            raw = row.get("PARTIDA")
            if pd.isna(raw) or str(raw).strip() == "":
                msg = (
                    f"Fila {idx} de DONACIONES tiene PARTIDA vacía/nula. "
                    "No se puede continuar."
                )
                logger.error(msg)
                raise RuntimeError(msg)

            nro = str(raw).strip()
            nro_norm = nro.lower()

            if nro_norm in cache:
                partida_ids.append(cache[nro_norm])
                continue

            rows = conn.execute(
                text(
                    "SELECT id FROM partidas "
                    "WHERE LOWER(TRIM(nro_partida)) = :nro"
                ),
                {"nro": nro_norm},
            ).fetchall()

            if len(rows) == 0:
                msg = (
                    f"No se encontró la partida '{nro}' en la tabla partidas "
                    f"(fila {idx} de DONACIONES). No se puede continuar."
                )
                logger.error(msg)
                raise RuntimeError(msg)

            if len(rows) > 1:
                msg = (
                    f"Se encontraron {len(rows)} partidas para '{nro}' "
                    f"en la tabla partidas (fila {idx} de DONACIONES). "
                    "Se esperaba exactamente 1. No se puede continuar."
                )
                logger.error(msg)
                raise RuntimeError(msg)

            pid = int(rows[0][0])
            cache[nro_norm] = pid
            partida_ids.append(pid)

    logger.info(
        f"Partidas resueltas: {len(partida_ids)} filas, "
        f"{len(cache)} partidas únicas"
    )
    return partida_ids


def run_all_validations(
    df_donaciones: pd.DataFrame, engine: Engine
) -> list[int]:
    """
    Ejecuta todas las validaciones y retorna la lista de partida_ids.
    Si cualquier validación falla, lanza excepción deteniendo todo.
    """
    validate_almacen(engine)
    validate_user(engine)
    return resolve_partida_ids(df_donaciones, engine)
