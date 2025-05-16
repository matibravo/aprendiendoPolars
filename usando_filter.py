'''Ejercicios
Selección de Columnas: Dado un DataFrame con las columnas 'ID', 'Nombre', 'Apellido', 'Edad', 'Ciudad' y 'País', crea un nuevo DataFrame que contenga solo las columnas 'Nombre', 'Apellido' y 'Edad'.
Filtrado de Filas: Dado el DataFrame anterior, filtra las filas donde la edad sea mayor o igual a 25 y la ciudad sea diferente de 'Nueva York'.
Combinación: Selecciona las columnas 'Nombre' y 'Edad' del DataFrame original y luego filtra las filas donde la edad sea menor de 30.
Uso de is_in: Dado un DataFrame con información de productos (Nombre, Categoría, Precio), filtra los productos cuya categoría sea 'Electrónicos' o 'Ropa'.'''

import polars as pl

# Crear un DataFrame
data = {
    'ID': [1,2,3],
    'Nombre': ['Alice', 'Bob', 'Charlie'],
    'Apellido': ['Bravo', 'Castro', 'Smith'],
    'Edad': [35, 28, 23],
    'Ciudad': ['Nueva York', 'Londres', 'París'],
    'Pais': ['USA', 'England', 'France']}

df = pl.DataFrame(data)
#crea un nuevo DataFrame que contenga solo las columnas 'Nombre', 'Apellido' y 'Edad'.
new_df = df.select(pl.col('Nombre'), pl.col('Apellido'), pl.col('Edad'))
print(new_df)

#Filtrado de Filas: Dado el DataFrame anterior, filtra las filas donde la edad sea mayor o igual a 25 y la ciudad sea diferente de 'Nueva York'.
new_df_filtro = df.filter((pl.col('Edad') >= 25) & (pl.col('Ciudad') != 'Nueva York'))
print(new_df_filtro)

#Combinación: Selecciona las columnas 'Nombre' y 'Edad' del DataFrame original y luego filtra las filas donde la edad sea menor de 30.
new_df_nombreEdad = df.select(pl.col('Nombre'), pl.col('Edad'))
new_df_menor30 = new_df_nombreEdad.filter(pl.col('Edad') < 30)
print(new_df_menor30) 

#Uso de is_in: Dado un DataFrame con información de productos (Nombre, Categoría, Precio), filtra los productos cuya categoría sea 'Electrónicos' o 'Ropa'.'''
datos_productos = {
    'Nombre': ['Laptop', 'Camisa', 'Red Polo'],
    'Categoria': ['Electronicos', 'Ropa', 'Perfumes'],
    'Precio': [900.000, 20.000, 195.000]
}

df_productos = pl.DataFrame(datos_productos)
print(df_productos)
categoria = ['Electronicos','Ropa']
df_productos_filtrados = df_productos.filter(pl.col('Categoria').is_in(categoria))
print(df_productos_filtrados)