import polars as pl
import time

data = {
    "nombre": ["Alice", "Bob", "Charlie"],
    "edad": [25, 30, 22],
    "ciudad": ["Nueva York", "Londres", "París"],
    "sexo": ["F", "M", "M"],
    "tipo": [1, 2, 1],
    "fecha_nacimiento": ["1998-01-01", "1993-05-15", "2001-07-20"]
}

df = pl.DataFrame(data)
print(df)

df1 = df.with_columns(pl.col("edad") * 2) #añade o modifica columnas

print(df1) 

'''schema = [("nombre", pl.Utf8), ("edad", pl.Int32), ("ciudad", pl.Utf8)]
df1 = pl.DataFrame(schema=schema)
print(df1)'''

data1 = {
    "nombre": ["Ana", "Juan", "Maria"],
    "edad": [28, 35, 30],
    "ciudad": ["Madrid", "Barcelona", "Valencia"],
}

dt_data1 = pl.DataFrame(data1)

dt_vacio = pl.DataFrame({
    "producto": pl.Utf8,
    "precio": pl.Float64,
})

dt_data1_renombrado = dt_data1.rename({"nombre": "Cliente", "edad": "Edad", "ciudad": "Ciudad"})

print(dt_data1)
print(dt_vacio)
print(dt_data1_renombrado)

#los dataframe son imnmutables, por lo que no se pueden modificar directamente. Se debe crear un nuevo dataframe con los cambios deseados.

#leyendo un csv 
dt_datos = pl.read_csv("datos.csv")
print(dt_datos)

#leer solo columnas especificas del csv
dt_datos_columnas_especificas = pl.read_csv("datos.csv", columns=["nombre", "edad"])
print(dt_datos_columnas_especificas)

#leer csv con separador distinto
df_datos_tienda = pl.read_csv("datos_tienda.csv", separator="|")
print(df_datos_tienda)

#seleccion de columnas y renombrar columna
df_datos1 = dt_datos.select(pl.col("nombre"), pl.col("email")).rename({"email": "correo electronico"})
print(df_datos1)

#crea un dataframe con las columnas q me interesan y renombro email, ayuda al uso de memoria de polars
# APLICAR EN CODIGO DE REPORTE
columnas_filtro = ["nombre", "email"]
df_datos2 = pl.read_csv("datos.csv", columns=columnas_filtro).rename({"email": "correo electronico"})
print(df_datos2)

#comprobacion de tiempo de lectura de csv 
start = time.perf_counter()

columnas_filtro_dos = ['conversation_id','conversation_start','conversation_end','originating_direction','division_ids']
df_datos3 = pl.read_csv("conversation_2025-04-17.csv", columns=columnas_filtro_dos)
print(df_datos3)

end = time.perf_counter()
print(f"Tiempo de ejecución: {end - start:.4f} segundos, con filtro")

start1 = time.perf_counter()

df_datos4 = pl.read_csv("conversation_2025-04-17.csv")
print(df_datos4)

end = time.perf_counter()
print(f"Tiempo de ejecución: {end - start:.4f} segundos, sin filtro")

#CONCLUSION: es mejor hacer la lectura de solo las columnas que se van a necesitar y no cargar todo el archivo en memoria ej:
'''columnas_filtro_dos = ['conversation_id','conversation_start','conversation_end','originating_direction','division_ids']
df_datos3 = pl.read_csv("conversation_2025-04-17.csv", columns=columnas_filtro_dos)
'''