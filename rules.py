import pandas as pd

FARMACIA_GRUPOS = [
    "PSICOTROPICOS",
    "PSICOFARMACOS",
    "ANTIBIOTICO Y OTROS",
    "INSUMOS",
    "DENTAL",
    "R.  X.",
    "LABORATORIO",
]


def _col0_str(df: pd.DataFrame) -> pd.Series:
    return df.iloc[:, 0].astype(str).fillna("").str.strip()


def clean_detalle(df: pd.DataFrame) -> pd.DataFrame:
    # reglas para todas las hojas detalle
    col0 = _col0_str(df)

    df = df.copy()
    df["PARTIDA_CODIGO"] = col0.str.extract(r"PARTIDA\s*N[°º]\s*(\d+)", expand=False)
    df["PARTIDA_CODIGO"] = df["PARTIDA_CODIGO"].ffill()

    mask_partida_header = col0.str.contains(
        r"^PARTIDA\s*N[°º]\s*\d+", case=False, regex=True
    )
    mask_total_partida = col0.str.contains(r"TOTAL\s+PARTIDA", case=False, na=False)
    mask_total_general = col0.str.contains(r"TOTAL\s+GENERAL", case=False, na=False)

    mask_codigo_vacio = df["CODIGO"].isna() | (
        df["CODIGO"].astype(str).str.strip() == ""
    )

    out = df[
        ~(
            mask_partida_header
            | mask_total_partida
            | mask_total_general
            | mask_codigo_vacio
        )
    ].copy()
    out.reset_index(drop=True, inplace=True)
    return out


def clean_farmacia(df: pd.DataFrame) -> pd.DataFrame:
    # hoja detalle farmacia con reglas especiales
    col0 = _col0_str(df)
    df = df.copy()

    grupos_set = set(g.strip() for g in FARMACIA_GRUPOS)
    mask_grupo_header = col0.isin(grupos_set)

    df["GRUPO"] = col0.where(mask_grupo_header)
    df["GRUPO"] = df["GRUPO"].ffill()

    df["PARTIDA_CODIGO"] = col0.str.extract(r"PARTIDA\s*N[°º]\s*(\d+)", expand=False)
    df["PARTIDA_CODIGO"] = df["PARTIDA_CODIGO"].ffill()

    mask_partida_header = col0.str.contains(
        r"^PARTIDA\s*N[°º]\s*\d+", case=False, regex=True
    )
    mask_total_any = col0.str.contains(r"\bTOTAL\b", case=False, regex=True)

    mask_codigo_vacio = df["CODIGO"].isna() | (
        df["CODIGO"].astype(str).str.strip() == ""
    )

    out = df[
        ~(mask_grupo_header | mask_partida_header | mask_total_any | mask_codigo_vacio)
    ].copy()
    out.reset_index(drop=True, inplace=True)
    return out


def clean_contable(df: pd.DataFrame) -> pd.DataFrame:
    # reglas para hojas contables
    df = df.copy()
    col0 = _col0_str(df)

    mask_total = col0.str.contains(r"\bTOTAL\b", case=False, regex=True)
    mask_totales = col0.str.contains(r"\bTOTALES\b", case=False, regex=True)

    out = df[~mask_total & ~mask_totales].copy()

    out = out.dropna(how="all").copy()

    out.reset_index(drop=True, inplace=True)
    return out
