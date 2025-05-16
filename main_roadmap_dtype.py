import polars as pl
import psutil
import time

# Mide el uso de CPU y memoria antes
cpu_before = psutil.cpu_percent(interval=1)
mem_before = psutil.virtual_memory().percent

try:
    #id_producto (entero), nombre_producto (texto), precio (decimal), en_stock (booleano)
    dt_productos = pl.read_csv("producto.csv", schema_overrides=[pl.Int64, pl.Utf8, pl.Float64, pl.Boolean])
    print(dt_productos.head(3))
    print(dt_productos.tail(2)) #por defecto es 5
    print(dt_productos.describe())
    
    # Mide después de leer el archivo
    cpu_after = psutil.cpu_percent(interval=1)
    mem_after = psutil.virtual_memory().percent

    print(f"\nUso de CPU antes: {cpu_before}% | después: {cpu_after}%")
    print(f"Uso de Memoria antes: {mem_before}% | después: {mem_after}%")
    
except Exception as e:
    print(f"ha ocurrido un error en el tipo de dato contenido en el dataframe: {e}")
    
 

