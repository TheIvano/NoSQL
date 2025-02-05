import pandas as pd
import json


def load_cleaned_json(paths):
    """
    Loads multiple cleaned JSON files into a dictionary of DataFrames.
    
    Parameters:
        paths (dict): Dictionary of file paths for cleaned JSON files.
        
    Returns:
        dict: A dictionary where keys are file names and values are DataFrames.
    """
    try:
        return {name: pd.read_json(path) for name, path in paths.items()}
    except Exception as e:
        print(f"Error loading JSON files: {e}")
        return {}


def group_and_embed_data(df, group_by_column, fields_to_embed):
    """
    Groups the data by a specified column and embeds relevant fields into nested dictionaries.
    
    Parameters:
        df (pd.DataFrame): The DataFrame to group.
        group_by_column (str): The column by which to group the data.
        fields_to_embed (list): List of fields to include in the nested dictionary.
        
    Returns:
        pd.DataFrame: The DataFrame with embedded fields.
    """
    return df.groupby(group_by_column).apply(
        lambda x: [
            {field: row[field] for field in fields_to_embed} for _, row in x.iterrows()
        ]
    ).reset_index(name=group_by_column.lower() + 's')


def construct_locales_document(row, terrazas, actividades):
    """
    Constructs a MongoDB document for a single `locales` entry.
    
    Parameters:
        row (pd.Series): A row from the `locales` DataFrame.
        terrazas (list): List of terrazas for this `locales` entry.
        actividades (list): List of actividades for this `locales` entry.
        
    Returns:
        dict: A MongoDB-compatible document for the `locales` entry.
    """
    return {
        "id_local": row["id_local"],
        "rotulo": row["rotulo"],
        "direccion": {
            "id_vial_edificio": row["id_vial_edificio"],
            "clase_vial_edificio": row["clase_vial_edificio"],
            "desc_vial_edificio": row["desc_vial_edificio"],
            "num_edificio": row["num_edificio"],
            "cal_edificio": row["cal_edificio"],
            "id_vial_acceso": row["id_vial_acceso"],
            "clase_vial_acceso": row["clase_vial_acceso"],
            "desc_vial_acceso": row["desc_vial_acceso"],
            "num_acceso": row["num_acceso"],
            "cal_acceso": row["cal_acceso"]
        },
        "ubicacion": {
            "id_distrito_local": row["id_distrito_local"],
            "desc_distrito_local": row["desc_distrito_local"],
            "id_barrio_local": row["id_barrio_local"],
            "desc_barrio_local": row["desc_barrio_local"],
            "cod_barrio_local": row["cod_barrio_local"],
            "id_seccion_censal_local": row["id_seccion_censal_local"],
            "desc_seccion_censal_local": row["desc_seccion_censal_local"],
            "coordenadas": {
                "coordenada_x_local": row["coordenada_x_local"],
                "coordenada_y_local": row["coordenada_y_local"]
            }
        },
        "situacion": {
            "id_situacion_local": row["id_situacion_local"],
            "desc_situacion_local": row["desc_situacion_local"],
            "id_tipo_acceso_local": row["id_tipo_acceso_local"],
            "desc_tipo_acceso_local": row["desc_tipo_acceso_local"]
        },
        "agrupacion": {
            "id_agrupacion": row["id_agrupacion"],
            "nombre_agrupacion": row["nombre_agrupacion"],
            "id_tipo_agrup": row["id_tipo_agrup"],
            "desc_tipo_agrup": row["desc_tipo_agrup"],
            "id_planta_agrupado": row["id_planta_agrupado"],
            "id_local_agrupado": row["id_local_agrupado"],
            "coordenadas_agrupacion": {
                "coordenada_x_agrupacion": row["coordenada_x_agrupacion"],
                "coordenada_y_agrupacion": row["coordenada_y_agrupacion"]
            }
        },
        "horarios": {
            "hora_apertura1": row.get("hora_apertura1", ""),
            "hora_cierre1": row.get("hora_cierre1", ""),
            "hora_apertura2": row.get("hora_apertura2", ""),
            "hora_cierre2": row.get("hora_cierre2", "")
        },
        "codigo_postal": row.get("cod_postal", ""),
        "edificio": {
            "id_ndp_edificio": row["id_ndp_edificio"],
            "nom_edificio": row["nom_edificio"],
            "secuencial_local_PC": row["secuencial_local_PC"]
        },
        "terrazas": terrazas,
        "actividades": actividades
    }


def generate_locales_and_licencias(locales, terrazas, actividades, licencias):
    """
    Generate `locales` and `licencias` collections for MongoDB from the provided data.
    
    Parameters:
        locales (pd.DataFrame): DataFrame containing `locales` data.
        terrazas (pd.DataFrame): DataFrame containing `terrazas` data.
        actividades (pd.DataFrame): DataFrame containing `actividades` data.
        licencias (pd.DataFrame): DataFrame containing `licencias` data.
        
    Returns:
        tuple: A tuple containing two lists of documents for `locales` and `licencias`.
    """
    # Group terrazas and actividades by `id_local`
    terrazas_grouped = group_and_embed_data(terrazas, "id_local", [
        "id_terraza", "id_periodo_terraza", "desc_periodo_terraza", "id_situacion_terraza", 
        "desc_situacion_terraza", "Superficie_ES", "Superficie_RA", "Fecha_confir_ult_decreto_resol",
        "id_ndp_terraza", "id_vial", "desc_clase", "desc_nombre", "nom_terraza", "num_terraza", 
        "cal_terraza", "desc_ubicacion_terraza", "hora_ini_LJ_es", "hora_fin_LJ_es", "hora_ini_LJ_ra",
        "hora_fin_LJ_ra", "hora_ini_VS_es", "hora_fin_VS_es", "hora_ini_VS_ra", "hora_fin_VS_ra",
        "mesas_aux_es", "mesas_aux_ra", "mesas_es", "mesas_ra", "sillas_es", "sillas_ra"
    ])
    
    actividades_grouped = group_and_embed_data(actividades, "id_local", [
        "id_seccion", "desc_seccion", "id_division", "desc_division", "id_epigrafe", "desc_epigrafe"
    ])

    # Merge the grouped terrazas and actividades with locales
    locales = locales.merge(terrazas_grouped, on="id_local", how="left").merge(actividades_grouped, on="id_local", how="left")

    # Replace NaN with empty lists
    locales["terrazas"] = locales["terrazas"].apply(lambda x: x if isinstance(x, list) else [])
    locales["actividades"] = locales["actividades"].apply(lambda x: x if isinstance(x, list) else [])

    # Create MongoDB documents for locales
    locales_docs = [
        construct_locales_document(row, row["terrazas"], row["actividades"]) for _, row in locales.iterrows()
    ]

    # Create MongoDB documents for licencias
    licencias_docs = [
        {
            "ref_licencia": row["ref_licencia"],
            "id_local": row["id_local"],
            "tipo": {
                "id_tipo_licencia": row["id_tipo_licencia"],
                "desc_tipo_licencia": row["desc_tipo_licencia"]
            },
            "situacion": {
                "id_tipo_situacion_licencia": row["id_tipo_situacion_licencia"],
                "desc_tipo_situacion_licencia": row["desc_tipo_situacion_licencia"]
            },
            "Fecha_Dec_Lic": row["Fecha_Dec_Lic"]
        }
        for _, row in licencias.iterrows()
    ]

    return locales_docs, licencias_docs


def save_to_json(data, path):
    """
    Saves the provided data to a JSON file.
    
    Parameters:
        data (list): The data to save.
        path (str): The output file path.
    """
    try:
        with open(path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
        print(f"Data saved to: {path}")
    except Exception as e:
        print(f"Error saving data to {path}: {e}")


if __name__ == "__main__":
    # Paths to cleaned JSON files
    cleaned_files = {
        "locales": "output/cleaned_locales.json",
        "terrazas": "output/cleaned_terrazas.json",
        "actividades": "output/cleaned_actividades.json",
        "licencias": "output/cleaned_licencias.json"
    }

    # Output paths for structured collections
    output_files = {
        "locales": "collections/structured_locales.json",
        "licencias": "collections/structured_licencias.json"
    }

    # Load cleaned data
    data = load_cleaned_json(cleaned_files)

    # Generate the structured collections
    locales_docs, licencias_docs = generate_locales_and_licencias(
        data["locales"], data["terrazas"], data["actividades"], data["licencias"]
    )

    # Save the generated collections
    save_to_json(locales_docs, output_files["locales"])
    save_to_json(licencias_docs, output_files["licencias"])
