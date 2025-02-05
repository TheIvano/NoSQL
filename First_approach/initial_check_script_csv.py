import pandas as pd
from collections import Counter

def analyze_csv(file_path):
    try:
        # Leer el archivo CSV
        df = pd.read_csv(file_path)

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

        # Detectar inconsistencias en los tipos de datos
        print("\n--- Inconsistencias de tipos de datos ---\n")
        for column in df.columns:
            inferred_types = df[column].apply(type).value_counts()
            if len(inferred_types) > 1:
                print(f"Columna '{column}' tiene múltiples tipos: {dict(inferred_types)}")

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

# Ruta del archivo CSV
file_path = "actividadeconomica202312/actividadeconomica202312.csv"  # Cambia esto por la ruta real

# Ejecutar el análisis
analyze_csv(file_path)