# manejar la carga y guardado de archivos CSV en la carpeta auxiliar
import pandas as pd
import glob
import os
from datetime import datetime
from config.database import BASE_AUXILIAR

def cargar_tablas_desde_auxiliar():
    #Carga las tablas desde la carpeta AUXILIAR
    dataframes = {}
    
    # Cargar directamente desde auxiliar (asume que existe)
    for ruta in glob.glob(os.path.join(BASE_AUXILIAR, "*.csv")):
        nombre = os.path.basename(ruta).replace(".csv", "")
        # Excluir el archivo de log
        if nombre != "log_cambios":
            dataframes[nombre] = pd.read_csv(ruta)
    
    print(f"📂 Cargadas {len(dataframes)} tablas desde auxiliar")
    return dataframes

def guardar_tabla_individual(df, nombre_tabla):
    #Guarda una tabla individual en la carpeta auxiliar
    os.makedirs(BASE_AUXILIAR, exist_ok=True)
    ruta_salida = os.path.join(BASE_AUXILIAR, f"{nombre_tabla}.csv")
    df.to_csv(ruta_salida, index=False)
    print(f"💾 Guardado en auxiliar: {ruta_salida}")

def guardar_tablas(dataframes, carpeta_salida):
    os.makedirs(carpeta_salida, exist_ok=True)
    for nombre, df in dataframes.items():
        ruta_salida = os.path.join(carpeta_salida, f"{nombre}.csv")
        df.to_csv(ruta_salida, index=False)
        print(f"✅ Guardado: {ruta_salida}")

