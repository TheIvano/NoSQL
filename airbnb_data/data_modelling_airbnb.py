import ast
import json
import pandas as pd


def load_data(input_csv_path):
    """
    Loads the cleaned Airbnb CSV dataset from the specified path.
    
    Parameters:
        input_csv_path (str): Path to the cleaned CSV file.
        
    Returns:
        pd.DataFrame: The loaded dataset.
    """
    try:
        # Open the file manually to handle encoding issues
        with open(input_csv_path, "r", encoding="utf-8", errors="replace") as file:
            df = pd.read_csv(file)
        return df
    except Exception as e:
        print(f"Error loading data from {input_csv_path}: {e}")
        return None


def replace_missing_values(df):
    """
    Replaces NaN values in the dataset with appropriate default values.
    
    Parameters:
        df (pd.DataFrame): The Airbnb dataset.
        
    Returns:
        pd.DataFrame: The dataset with NaN values replaced.
    """
    fill_defaults = {
        "name": "",
        "host_id": "",
        "host_name": "",
        "host_since": "",
        "host_is_superhost": False,
        "host_response_rate": 0,
        "host_acceptance_rate": 0,
        "host_listings_count": 0,
        "neighbourhood": "Desconocido",
        "latitude": 0.0,
        "longitude": 0.0,
        "property_type": "",
        "room_type": "",
        "accommodates": 0,
        "bathrooms": 0,
        "bedrooms": 0,
        "beds": 0,
        "amenities": "",
        "price": 0.0,
        "minimum_nights": 0,
        "maximum_nights": 0,
        "availability_365": 0,
        "number_of_reviews": 0,
        "review_scores_rating": 0.0,
        "review_scores_cleanliness": 0.0,
        "review_scores_accuracy": 0.0,
        "review_scores_communication": 0.0,
        "review_scores_location": 0.0,
        "review_scores_value": 0.0,
        "instant_bookable": False,
        "reviews_per_month": 0.0,
        "calculated_host_listings_count": 0
    }
    return df.fillna(fill_defaults)


def normalize_columns(df):
    """
    Normalizes specific columns like price and percentage values.
    
    Parameters:
        df (pd.DataFrame): The Airbnb dataset.
        
    Returns:
        pd.DataFrame: The dataset with normalized columns.
    """
    # Normalize price column to float
    df['price'] = df['price'].replace({r'[$,]': ''}, regex=True).astype(float)

    # Convert percentage columns to integers
    percentage_columns = ["host_response_rate", "host_acceptance_rate"]
    for col in percentage_columns:
        df[col] = df[col].replace({r'%': ''}, regex=True).astype(int)

    # Convert specific columns to integers
    integer_columns = ["accommodates", "bathrooms", "bedrooms", "beds", "availability_365", "host_listings_count"]
    for col in integer_columns:
        df[col] = df[col].astype(int)

    return df


def parse_amenities(amenities_str):
    """
    Parses the amenities string into a list of amenities.
    
    Parameters:
        amenities_str (str): The amenities column value as a string.
        
    Returns:
        list: A list of amenities or an empty list if parsing fails.
    """
    amenities = []
    if isinstance(amenities_str, str):
        try:
            # Parse the string as a Python list
            amenities = ast.literal_eval(amenities_str)
            # Clean up any extra quotes or whitespace
            amenities = [item.strip().replace('"', '') for item in amenities]
        except (ValueError, SyntaxError):
            amenities = []  # Default to an empty list if parsing fails
    return amenities


def transform_to_json(df):
    """
    Transforms the cleaned DataFrame into a structured JSON format.
    
    Parameters:
        df (pd.DataFrame): The cleaned and processed Airbnb dataset.
        
    Returns:
        list: A list of transformed listings in JSON format.
    """
    listings = []
    for _, row in df.iterrows():
        amenities = parse_amenities(row.get("amenities", ""))
        
        listing = {
            "id": row["id"],
            "name": row.get("name", ""),
            "host": {
                "host_id": row.get("host_id", ""),
                "host_name": row.get("host_name", ""),
                "host_since": row.get("host_since", ""),
                "host_is_superhost": row.get("host_is_superhost", False),
                "host_response_rate": row.get("host_response_rate", 0),
                "host_acceptance_rate": row.get("host_acceptance_rate", 0),
                "host_listings_count": row.get("host_listings_count", 0),
            },
            "location": {
                "neighbourhood": row.get("neighbourhood", ""),
                "neighbourhood_cleansed": row.get("neighbourhood_cleansed", ""),
                "neighbourhood_group_cleansed": row.get("neighbourhood_group_cleansed", ""),
                "latitude": row.get("latitude", 0.0),
                "longitude": row.get("longitude", 0.0),
            },
            "details": {
                "property_type": row.get("property_type", ""),
                "room_type": row.get("room_type", ""),
                "accommodates": row.get("accommodates", 0),
                "bathrooms": row.get("bathrooms", 0),
                "bedrooms": row.get("bedrooms", 0),
                "beds": row.get("beds", 0),
                "amenities": amenities
            },
            "pricing": {
                "price": row.get("price", 0.0),
                "minimum_nights": row.get("minimum_nights", 0),
                "maximum_nights": row.get("maximum_nights", 0),
                "availability_365": row.get("availability_365", 0),
            },
            "reviews": {
                "number_of_reviews": row.get("number_of_reviews", 0),
                "scores": {
                    "review_scores_rating": row.get("review_scores_rating", 0.0),
                    "review_scores_cleanliness": row.get("review_scores_cleanliness", 0.0),
                    "review_scores_accuracy": row.get("review_scores_accuracy", 0.0),
                    "review_scores_communication": row.get("review_scores_communication", 0.0),
                    "review_scores_location": row.get("review_scores_location", 0.0),
                    "review_scores_value": row.get("review_scores_value", 0.0)
                }
            }
        }
        listings.append(listing)
    return listings


def save_to_json(data, output_json_path):
    """
    Saves the transformed data into a JSON file.
    
    Parameters:
        data (list): The transformed data to save.
        output_json_path (str): The path to save the JSON file.
    """
    try:
        with open(output_json_path, "w", encoding="utf-8", errors="replace") as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
        print(f"Transformed listings saved to: {output_json_path}")
    except Exception as e:
        print(f"Error saving transformed data to {output_json_path}: {e}")


def transform_cleaned_listings(input_csv_path, output_json_path):
    """
    Transforms a cleaned Airbnb CSV dataset into a structured JSON format.
    
    Parameters:
        input_csv_path (str): Path to the cleaned CSV file.
        output_json_path (str): Path to save the transformed JSON file.
    """
    # Load data
    df = load_data(input_csv_path)
    if df is None:
        return

    # Replace missing values
    df = replace_missing_values(df)

    # Normalize columns
    df = normalize_columns(df)

    # Transform to structured JSON
    listings_json = transform_to_json(df)

    # Save to JSON file
    save_to_json(listings_json, output_json_path)


if __name__ == "__main__":
    # Define input and output paths
    input_path = "output/cleaned_listings.csv"
    output_path = "collections/structured_alojamientos.json"
    
    # Execute the transformation
    transform_cleaned_listings(input_path, output_path)
