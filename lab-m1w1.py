import duckdb
import requests
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os
import pdb

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
    
    # Fetch Pokemon names
    pokemon_data = []
    for pokemon in results:
        pokemon_data.append({
            "name": pokemon["name"]  # Only store name as unique identifier
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


def load_data(parquet_file="pokemon_data.parquet", db_file="pokedex.db", conn=None):
    """
    Loads extracted Pokémon data from a Parquet file into DuckDB and tracks the last extracted Pokémon ID.
    
    Args:
        parquet_file (str): Path to the Parquet file.
        db_file (str): Path to the DuckDB database file.
    """
    if conn is None:
        conn = duckdb.connect(db_file)
    
    # Create pokedex table if it doesn't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pokedex (
            name TEXT PRIMARY KEY
        )
    """)
    
    # Create metadata table if it doesn't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """
    )
    
    # Get last extracted Pokémon name
    last_name = conn.execute("SELECT value FROM metadata WHERE key = 'last_extracted_name'").fetchone()
    last_name = last_name[0] if last_name else None
    
    # Load new data from Parquet
    df = pd.read_parquet(parquet_file)
    if last_name:
        df = df[df["name"] > last_name]  # Filter out already loaded data
    
    if not df.empty:
        # Insert new data into DuckDB
        conn.execute("INSERT INTO pokedex SELECT * FROM df")
        
        # Update metadata table with the new last name
        new_last_name = df["name"].max()  # Assuming names are ordered
        conn.execute("""
            INSERT INTO metadata (key, value) VALUES ('last_extracted_name', ?) 
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (new_last_name,))
    
    return conn


def transform_data(conn, db_file="pokedex.db"):
    """
    Performs aggregations on Pokémon data and stores the results in a pokemon_stats table.
    
    Args:
        db_file (str): Path to the DuckDB database file.
    """
    # Create pokemon_stats table if it doesn't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pokemon_stats (
            total_pokemon INTEGER,
            first_pokemon_name TEXT,
            last_pokemon_name TEXT
        )
    """)
    
    # Calculate statistics
    stats = conn.execute("""
        SELECT COUNT(*) AS total_pokemon, MIN(name) AS first_pokemon_name, MAX(name) AS last_pokemon_name FROM pokedex
    """).fetchone()
    
    # Insert or update the pokemon_stats table
    conn.execute("""
        INSERT INTO pokemon_stats (total_pokemon, first_pokemon_name, last_pokemon_name)
        VALUES (?, ?, ?)
        ON CONFLICT DO UPDATE SET
        total_pokemon = excluded.total_pokemon,
        first_pokemon_name = excluded.first_pokemon_name,
        last_pokemon_name = excluded.last_pokemon_name
    """, stats)
    
    return conn

def main():
    # Create a DuckDB connection
    conn = duckdb.connect("pokedex.db")
    
    try:
        # Extract data twice to generate two Parquet files
        file_path, offset = extract_data(offset=0, limit=10)
        file_path, offset = extract_data(offset=offset, limit=10)  # Incremental extraction
        
        # Load both files into DuckDB
        conn = load_data(parquet_file=file_path, conn=conn)
        conn = load_data(parquet_file=file_path, conn=conn)  # Load the second file
        
        # Transform the data
        conn = transform_data(conn, db_file="pokedex.db")
        
    finally:
        # Close the connection
        conn.close()

    print("ELT Pipeline executed successfully.")
#Show me the result, the table in the end"