"""
utils.logger
=============
Configuración de logging para el ETL: consola + archivo.
Uso: from utils.logger import get_logger; logger = get_logger()
"""

import logging
import os

_LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "output")
_LOG_FILE = os.path.join(_LOG_DIR, "etl_migration.log")
_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"

_LOGGER: logging.Logger | None = None


def get_logger(name: str = "etl_migration") -> logging.Logger:
    """Devuelve un logger con FileHandler (output/etl_migration.log) y StreamHandler (consola)."""
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER

    os.makedirs(_LOG_DIR, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(_FORMAT, datefmt=_DATE_FMT)

    fh = logging.FileHandler(_LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    _LOGGER = logger
    return logger
