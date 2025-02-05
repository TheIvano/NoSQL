import pandas as pd
import json

def load_json_file(input_path):
    """
    Loads a JSON file into a pandas DataFrame.
    """
    try:
        with open(input_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return pd.DataFrame()

def save_cleaned_data(df, output_path):
    """
    Saves the cleaned DataFrame to a JSON file.
    """
    try:
        cleaned_data = df.to_dict(orient='records')
        with open(output_path, 'w', encoding='utf-8') as file:
            json.dump(cleaned_data, file, ensure_ascii=False, indent=2)
        print(f"Cleaned file saved at: {output_path}")
    except Exception as e:
        print(f"Error saving cleaned data: {e}")

def remove_single_value_columns(df):
    """
    Removes columns that have only one unique value.
    """
    single_value_cols = [col for col in df.columns if df[col].nunique() <= 1]
    print(f"Removing single-value columns: {single_value_cols}")
    return df.drop(columns=single_value_cols)

def remove_unnecessary_fields(df, unnecessary_fields):
    """
    Removes unnecessary fields from the DataFrame.
    """
    unnecessary_fields = [field for field in unnecessary_fields if field in df.columns]
    print(f"Removing unnecessary fields: {unnecessary_fields}")
    return df.drop(columns=unnecessary_fields)

def update_actividad_fields(df):
    """
    Updates specific fields for 'actividad' based on business logic.
    """
    if "id_seccion" in df.columns and "id_division" in df.columns and "id_epigrafe" in df.columns:
        df.loc[(df["id_seccion"] == "-1"), "desc_seccion"] = ""
        df.loc[(df["id_division"] == "-1"), "desc_division"] = ""
        df.loc[(df["id_epigrafe"] == "-1"), "desc_epigrafe"] = ""
    return df

def update_agrupacion_fields(df):
    """
    Updates specific fields for 'agrupación' based on business logic.
    """
    if "id_agrupacion" in df.columns and "id_tipo_agrup" in df.columns:
        df.loc[(df["id_agrupacion"] == -1), "nombre_agrupacion"] = ""
        df.loc[(df["id_tipo_agrup"] == -1), "desc_tipo_agrup"] = ""
    return df

def update_rotulo_field(df):
    """
    Updates specific values for the 'rotulo' field.
    """
    if "rotulo" in df.columns:
        invalid_rotulos = [
            "SIN ACTIVIDAD", "SIN NOTIFICAR", "SIN ROTULO",
            "SIN INDICAR", "SIN ESPECIFICAR", "SIN DEFINIR", "SIN DETERMINAR"
        ]
        df.loc[df["rotulo"].isin(invalid_rotulos), "rotulo"] = ""
    return df

def clean_json_file(input_path, output_path, unnecessary_fields=["fx_carga"], integer_fields=["mesas_aux_ra", "sillas_ra", "mesas_ra"]):
    """
    Cleans a JSON file by performing a series of data transformations and saves it.
    """
    try:
        df = load_json_file(input_path)
        if df.empty:
            return

        # Clean data
        df = df.apply(lambda col: col.str.strip() if col.dtype == 'object' else col)
        df = remove_single_value_columns(df)
        df = remove_unnecessary_fields(df, unnecessary_fields)
        df = update_actividad_fields(df)
        df = update_agrupacion_fields(df)
        df = update_rotulo_field(df)
        df["desc_barrio_local"] = df["desc_barrio_local"].str.lower()

        # Ensure specific numeric fields are integers
        for field in integer_fields:
            if field in df.columns:
                df[field] = df[field].fillna(0).astype(int)  # Replace NaN with 0 and cast to int

        # Replace all NaN values with empty strings
        df = df.fillna("")

        # Save the cleaned data
        save_cleaned_data(df, output_path)

    except Exception as e:
        print(f"An error occurred while cleaning the file: {e}")

if __name__ == "__main__":
    files_to_clean = {
        "actividades": ("json_files/actividadeconomica202312/actividadeconomica202312.json", "output/cleaned_actividades.json"),
        "licencias": ("json_files/licencias202312/licencias202312.json", "output/cleaned_licencias.json"),
        "locales": ("json_files/locales202312/locales202312.json", "output/cleaned_locales.json"),
        "terrazas": ("json_files/terrazas202312.json", "output/cleaned_terrazas.json"),
    }

    for file_name, (input_path, output_path) in files_to_clean.items():
        print(f"Cleaning file: {file_name}")
        clean_json_file(input_path, output_path)
