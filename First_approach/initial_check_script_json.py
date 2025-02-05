import json
import pandas as pd
from collections import *

def analyze_json(file_path):
    try:
        # Leer el archivo JSON
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        # Asegurarnos de que los datos son una lista de registros
        if isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            raise ValueError("El archivo JSON no contiene un formato compatible (lista o diccionario).")

        # Convertir a un DataFrame para facilitar el análisis
        df = pd.DataFrame(data)

        print("\n--- Análisis inicial ---\n")
        print(f"Número de registros: {len(df)}")
        print(f"Número de columnas: {len(df.columns)}")

        print("\n--- Campos y tipos de datos ---\n")
        print(df.dtypes)

        # Identificar valores nulos
        print("\n--- Valores nulos por columna ---\n")
        print(df.isnull().sum())

        # Identificar duplicados
        print("\n--- Duplicados ---\n")
        num_duplicates = df.duplicated().sum()
        print(f"Número de filas duplicadas: {num_duplicates}")

        # Evaluar la calidad de los datos
        print("\n--- Evaluación de calidad de los datos ---\n")
        for column in df.columns:
            print(f"\nColumna: {column}")
            unique_values = df[column].nunique(dropna=True)
            total_values = len(df[column])
            null_values = df[column].isnull().sum()
            most_common = Counter(df[column].dropna()).most_common(1)

            print(f"\tValores únicos: {unique_values}")
            print(f"\tValores nulos: {null_values} ({(null_values / total_values) * 100:.2f}%)")
            if most_common:
                print(f"\tValor más común: {most_common[0][0]} (Frecuencia: {most_common[0][1]})")

    except Exception as e:
        print(f"Ocurrió un error: {e}")


# Ruta del archivo JSON
file_path = "locales202312/locales202312.json"

# Ejecutar el análisis
analyze_json(file_path)