import polars as pl
import datetime as dt


df = pl.DataFrame({
    "name": ["juan morales", "hugo sanchez", "paco ruiz", "luis jara"],
    "birthdate": [
        dt.date(1990, 5, 20),
        dt.date(1980, 7, 10),
        dt.date(1999, 3, 2),
        dt.date(1970, 8, 31),
    ],
    "weight": [60, 78, 84, 88],
    "height": [1.60, 1.89, 1.78, 1.90],
    "pais": ["Chile", "Peru", "Venzuela", " Cuba"]
})

#crea un archivo csv con el dataframe creado anteriormente
df.write_csv("./output.csv")
df_csv = pl.read_csv("./output.csv", try_parse_dates=True)

#se selecciona la columna nombre
#la columna nacimiento se deja solo el año
#se crea una columna bmi y se calcula el indice de masa corporal

result = df.select(
    pl.col("name"),
    pl.col("birthdate").dt.year().alias("año"),
    (pl.col("weight") / (pl.col("height") ** 2)).alias("masa corporal")
    
)

#agrega un sufijo al nombre de la columna por ejemplo que se le resta el 5 % a las columnas peso y altura

result_con_sufijo_nombre_columna = df.select(
    pl.col("name", "birthdate"),
    (pl.col("weight", "height") * 0.95).round(2).name.suffix("-5%")
)

#agregar columnas a nuestro dataframe base se usa with_columns

def evalua_masa_corporal(masa_corporal):
    print(masa_corporal)
    return(masa_corporal)

result_agrega_columnas = df.with_columns(
    year = pl.col("birthdate").dt.year(),
    masa_corporal = (pl.col("weight") / (pl.col("height") ** 2)).round(1)    
)

#usar filter crea otro dt con el filtro realizado
result_new_dt = df.filter(pl.col("birthdate").dt.year() < 1990)

#filtrar por fecha y altura
result_filtrar_fecha_altura = df.filter(
    pl.col("birthdate").is_between(dt.date(1990, 9, 10), dt.date(2000, 4, 10)),
    pl.col("height") > 1.5
)

#agrupar con group by como por decadas
result_group_by= df.group_by(
    (pl.col("birthdate").dt.year() // 10 * 10).alias("decada"),
    maintain_order=True,
).len()

#agg para calcular agregaciones sobre los grupos resultantes
result_agg = df.group_by(
    (pl.col("birthdate").dt.year() // 10 * 10).alias("Decada"),
    maintain_order=True
).agg(
    pl.len().alias("ejemplo de cantidad"),
    pl.col("weight").mean().round(1).alias("promedio de peso"),
    pl.col("height").max().alias("mas alto")
)

#ejemplo de consulta compleja
#luego del group by siempre va un agg

resulta_query_complex = (
    df.with_columns(
        (pl.col("birthdate").dt.year() // 10 * 10).alias("decada"),
        pl.col("name").str.split(by=" ").list.first()
        )
        .select(
            pl.all().exclude("birthdate")
        )
        .group_by( #con group by le dices que quieres agrupar los registros en la columna decada
            pl.col("decada"),
            maintain_order=True
        )
        .agg( #le dice que quiero hace con cada grupo, como contar, sumar , promediar
            pl.len().alias("cantidad"),
            pl.col("name"),
            pl.col("height", "weight").mean().round(2).name.prefix("average_")
        )        
)

#uniendo dos dataframe  

#se crea segundo dataframe  
df2 = pl.DataFrame({
    "name": ["juan morales", "hugo sanchez", "paco ruiz", "luis jara", "pancho ortega"],
    "parent": [True, False, False, False, True],
    "siblings": [1, 2, 3, 4, 5],
})

#se une df con df2 con conexion en name y el df 2
#how="outer" muestra todas las filas de ambas dataframe
#how="left" muestra todas las filas de df y completa con las filas de df2 si hay match
#how="right" muestra todas las filas de df y completa con las filas de df si hay match
#how="inner" muestra solo las filas que hagan match
dataframe_merge = df.join(df2, on="name", how="left") 

#concatenar con otro dataframe
#seria como agregar mas registros en este ejemplo, ya que tiene la misma estructura(cantidad de columnas) que el principal dataframe
df3 = pl.DataFrame(
    {
        "name": ["Ethan Edwards", "Fiona Foster", "Grace Gibson", "Henry Harris"],
        "birthdate": [
            dt.date(1977, 5, 10),
            dt.date(1975, 6, 23),
            dt.date(1973, 7, 22),
            dt.date(1971, 8, 3),
        ],
        "weight": [67, 72, 57, 93],  # (kg)
        "height": [1.76, 1.6, 1.66, 1.8],  # (m)   
        "pais": ["Chile", "Peru", "Venzuela", " Cuba"]     
    }
)

dataframe_concatenado = pl.concat([df, df3], how="vertical")

dataframe_merge.write_csv("./output.csv")

print(df)
print("------------------->")
print(df_csv)
print("------------------->")
print(result)
print("------------------->")
print(result_con_sufijo_nombre_columna)
print("------------------->")
print(result_agrega_columnas)
print("------------------------------->")
print(result_new_dt)
print("------------------------------->")
print(result_filtrar_fecha_altura)
print("------------------------------->")
print(result_group_by)
print("------------------------------->")
print(result_agg)
print("------------------------------->")
print(resulta_query_complex)
print("------------------------------->")
print(df2)
print("------------------------------->")
print(dataframe_merge)
print("------------------------------->")
print(dataframe_concatenado)