import pandas as pd

def load_data(input_path):
    """
    Loads the Airbnb dataset from the specified CSV file.
    
    Parameters:
        input_path (str): Path to the raw Airbnb CSV file.
        
    Returns:
        pd.DataFrame: The loaded dataset.
    """
    try:
        df = pd.read_csv(input_path)
        print("\n=== Initial Dataset Overview ===")
        print(df.info())
        return df
    except Exception as e:
        print(f"Error loading data from {input_path}: {e}")
        return None


def remove_unnecessary_columns(df):
    """
    Removes unnecessary columns from the dataset.
    
    Parameters:
        df (pd.DataFrame): The Airbnb dataset.
        
    Returns:
        pd.DataFrame: The dataset with unnecessary columns removed.
    """
    unnecessary_columns = [
        "listing_url", "scrape_id", "last_scraped", "picture_url", "host_url", "host_thumbnail_url",
        "host_picture_url", "license", "calendar_updated", "calendar_last_scraped"
    ]
    return df.drop(columns=[col for col in unnecessary_columns if col in df.columns], errors='ignore')


def handle_missing_values(df):
    """
    Fills missing values in the dataset.
    
    Parameters:
        df (pd.DataFrame): The Airbnb dataset.
        
    Returns:
        pd.DataFrame: The dataset with missing values filled.
    """
    fill_defaults = {
        "neighbourhood": "Desconocido",
        "price": 0
    }
    df = df.fillna(value=fill_defaults)
    df = df.dropna(subset=["latitude", "longitude", "neighbourhood_cleansed"])
    print(f"\nDropped rows with missing latitude/longitude. Remaining rows: {len(df)}")
    return df


def normalize_text(df):
    """
    Normalizes text in categorical columns (e.g., lowercasing, stripping whitespace).
    
    Parameters:
        df (pd.DataFrame): The Airbnb dataset.
        
    Returns:
        pd.DataFrame: The dataset with normalized text in categorical columns.
    """
    text_columns = ["neighbourhood", "room_type", "property_type", "name"]
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].str.strip().str.lower()
    print("\nNormalized text in categorical columns.")
    return df


def remove_duplicates(df):
    """
    Removes duplicate rows based on key columns.
    
    Parameters:
        df (pd.DataFrame): The Airbnb dataset.
        
    Returns:
        pd.DataFrame: The dataset with duplicates removed.
    """
    df = df.drop_duplicates(subset=["latitude", "longitude", "name"])
    print(f"\nRemoved duplicates. Remaining rows: {len(df)}")
    return df


def save_cleaned_data(df, output_path):
    """
    Saves the cleaned dataset to a specified CSV file.
    
    Parameters:
        df (pd.DataFrame): The cleaned Airbnb dataset.
        output_path (str): The path where the cleaned dataset will be saved.
    """
    try:
        df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"\nCleaned dataset saved to: {output_path}")
    except Exception as e:
        print(f"Error saving cleaned data to {output_path}: {e}")


def clean_airbnb_data(input_path, output_path):
    """
    Cleans the Airbnb dataset by calling various helper functions in a pipeline.
    
    Parameters:
        input_path (str): Path to the raw Airbnb CSV file.
        output_path (str): Path to save the cleaned dataset.
    """
    # Load the dataset
    df = load_data(input_path)
    if df is None:
        return
    
    # Remove unnecessary columns
    df = remove_unnecessary_columns(df)
    
    # Handle missing values
    df = handle_missing_values(df)
    
    # Normalize text in categorical columns
    df = normalize_text(df)
    
    # Remove duplicates based on key columns
    df = remove_duplicates(df)
    
    # Save the cleaned dataset
    save_cleaned_data(df, output_path)


if __name__ == "__main__":
    input_file = "listings.csv"
    output_file = "output/cleaned_listings.csv"
    
    # Clean the dataset
    clean_airbnb_data(input_file, output_file)
