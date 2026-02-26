import pandas as pd

DEFAULT_MAX_WIDTH = 25  # límite general para columnas
DEFAULT_ROW_HEIGHT = 22  # alto de fila para wrap


def export_book_to_excel(
    dfs_by_sheet: dict[str, pd.DataFrame],
    output_path: str = "salida_limpia_formateada.xlsx",
    max_width: int = DEFAULT_MAX_WIDTH,
):
    """
    Exporta un diccionario {nombre_hoja: DataFrame} a un Excel con formato.
    - Todas las columnas tienen ancho limitado (max_width)
    - La columna DESCRIPCION (o la que indiques) tiene ancho mayor (descripcion_width)
    - wrap_text para que el texto largo no se sobreponga
    - freeze header, tabla con estilo (sin duplicar autofilter)
    """

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        for sheet_name, df in dfs_by_sheet.items():
            # evitar error si la hoja está vacía o None
            if df is None:
                continue

            # Excel no permite nombres > 31 chars
            safe_sheet = sheet_name[:31]

            df.to_excel(writer, sheet_name=safe_sheet, index=False)

            workbook = writer.book
            worksheet = writer.sheets[safe_sheet]

            # formatos
            header_fmt = workbook.add_format(
                {
                    "bold": True,
                    "font_color": "white",
                    "bg_color": "#1F4E79",
                    "border": 1,
                    "align": "center",
                    "valign": "vcenter",
                    "text_wrap": True,
                }
            )

            cell_fmt = workbook.add_format(
                {
                    "border": 1,
                    "valign": "top",
                    "text_wrap": True,
                }
            )

            # congelar encabezado
            worksheet.freeze_panes(1, 0)

            # headers con formato
            for col_idx, col_name in enumerate(df.columns):
                worksheet.write(0, col_idx, col_name, header_fmt)

            # ajustar ancho con límite
            for col_idx, col_name in enumerate(df.columns):
                
                # calculo ancho pero con límite
                series_as_str = df[col_name].astype(str).fillna("")
                max_len = max(
                    [len(str(col_name))] + series_as_str.map(len).tolist()
                )
                width = min(max_len + 2, max_width)

                worksheet.set_column(col_idx, col_idx, width, cell_fmt)

            # altura filas (para wrap)
            for row in range(1, len(df) + 1):
                worksheet.set_row(row, DEFAULT_ROW_HEIGHT)

            # tabla con estilo (esto ya crea autofilter)
            rows = len(df)
            cols = len(df.columns)
            if rows > 0 and cols > 0:
                worksheet.add_table(
                    0,
                    0,
                    rows,
                    cols - 1,
                    {
                        "style": "Table Style Medium 9",
                        "columns": [{"header": c} for c in df.columns],
                    },
                )

    # export_book_to_excel(
    #     dfs_limpios,
    #     output_path="salida_limpia_formateada.xlsx",
    #     descripcion_col="DESCRIPCION",
    #     max_width=11,  # tope para columnas normales
    #     descripcion_width=70,  # DESCRIPCION más ancha
    # )
