import polars as pl

try:
    df_producto_a = pl.read_csv("productos_a.csv")
    df_producto_b = pl.read_csv("productos_b.csv")
    df_producto_c = pl.read_csv("productos_c.csv")

    # Encuentra todas las columnas únicas entre los DataFrames
    all_cols = set(df_producto_a.columns) | set(df_producto_b.columns) | set(df_producto_c.columns)
    print(all_cols)

#aqui se crea funcion que al no encontrar la columna en el dataframe la agrega con valor None
    def align_columns(df, all_cols):
        for col in all_cols:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))        
        return df.select(sorted(all_cols))

    df1_aligned = align_columns(df_producto_a, all_cols)
    df2_aligned = align_columns(df_producto_b, all_cols)
    df3_aligned = align_columns(df_producto_c, all_cols)

    # Combinamos los DataFrames de forma vertical(hacia abajo)
    # para que esto funcione deben tener la misma cantidad de columnas 
    df_combinado = pl.concat([df1_aligned, df2_aligned, df3_aligned])

    print(df_combinado)
except:
    print("Ha ocurrido un error")