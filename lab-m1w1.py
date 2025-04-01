import duckdb
import requests
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os
import pdb
from time import sleep

def extract_data(offset=0, limit=10, file_path="pokemon_data.parquet"):

    url = f"https://pokeapi.co/api/v2/pokemon?limit={limit}&offset={offset}"
    print("Getting data from:", url)
    response = requests.get(url=url, timeout=10)
    response.raise_for_status()
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code}")
    
    data = response.json()
    results = data.get("results", [])
    
    if not results:
        return file_path, offset  # No more data to fetch
    
    # Fetch Pokemon names and url
    pokemon_data = []
    for pokemon in results:
        pokemon_data.append({
            "name": pokemon["name"],
            "url": pokemon["url"]  # Ensure URL is saved
        })
    
    # Convert results to DataFrame
    df = pd.DataFrame(pokemon_data)
    
    # Load existing data if the file already exists
    try:
        existing_df = pd.read_parquet(file_path)
        df = pd.concat([existing_df, df], ignore_index=True)
    except FileNotFoundError:
        pass  # File does not exist yet, so we create it fresh
    
    # Save to Parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)
    
    # Return file path and updated offset
    return file_path, offset + limit

"""
# Load the stored Parquet file
extract_data()
file_path = "pokemon_data.parquet"
df = pd.read_parquet(file_path)

# Load the first few rows
print(df.head())
"""

def load_data(file_path, new_last_id):
    conn = duckdb.connect("pokedex.db")

    conn.execute("DROP TABLE IF EXISTS pokedex")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pokedex (
            last_id INTEGER, 
            pokemon_id INTEGER, 
            name TEXT, 
            url TEXT
        )
    """)

    # Insert data into the table from Parquet
    conn.execute("""
        INSERT INTO pokedex (last_id, pokemon_id, name, url) 
        SELECT 
            ? as last_id,
            CAST(regexp_extract(url, '/pokemon/(\d+)/', 1) AS INTEGER) as pokemon_id,
            name,
            url 
        FROM read_parquet(?)
    """, [new_last_id, file_path])

    # Preview loaded data to check it
    preview_df = conn.execute("""
        SELECT * 
        FROM pokedex 
        WHERE last_id = ? 
        LIMIT 5
    """, [new_last_id]).fetchdf()

    print("Preview loaded data: ", preview_df)

    # Ensure metadata table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT)
    """)

    # Update the last extracted ID in the metadata table
    conn.execute("""
        INSERT INTO metadata (key, value) 
        VALUES ('last_extracted_id', ?) 
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """, [new_last_id])

    print("Data loaded successfully")
    
    return conn



def transform_data(conn):
    """
    Transform data from the raw pokedex table and create aggregated statistics.
    This function:
    1. Creates a new table 'pokemon_stats' with aggregated metrics.
    2. Calculates total count of pokemon, minimum and maximum pokemon IDs.
    3. Prints a preview of the transformed data.
    
    Returns:
        str: Name of the created table ('pokemon_stats')
    """
    # Create pokemon_stats table with aggregated statistics
    conn.execute("""
        CREATE OR REPLACE TABLE pokemon_stats AS
        SELECT 
            COUNT(*) AS total_pokemon, 
            MIN(pokemon_id) AS first_id, 
            MAX(pokemon_id) AS last_id
        FROM pokedex
    """)
    
    # Preview the transformed data by fetching the first few rows
    df = conn.execute("SELECT * FROM pokemon_stats").fetchdf()
    print("Preview transformed data: ", df)
    
    # Return the name of the created table
    return "pokemon_stats"


def main():
    print(" Extracting data...")
    file_path_1, new_last_id_1 = extract_data(limit=10, offset=0)  # First extraction
    print(f"Extracted data from offset {0} to {new_last_id_1}")
    
    print(" Extracting more data...")
    file_path_2, new_last_id_2 = extract_data(limit=10, offset=new_last_id_1)  # Second extraction
    print(f"Extracted data from offset {new_last_id_1} to {new_last_id_2}")
    
    print("Loading data...")
    load_data(file_path_1, new_last_id_1)  # Load the first batch
    load_data(file_path_2, new_last_id_2)  # Load the second batch
    
    print("Transforming data...")
    conn = duckdb.connect("pokedex.db")  # Ensure you have the connection ready
    transform_data(conn)  # Transform the data in the database
    
    print("ELT Pipeline completed successfully!")


if __name__ == "__main__":
    main()

