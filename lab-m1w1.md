# Lab for our Month 1 > Week 1

Hands-on: ELT Pipeline Development

🕥 Duration: 3 hours

- [Lab for our Month 1 \> Week 1](#lab-for-our-month-1--week-1)
  - [🎯 Objective](#-objective)
  - [🛠️ Prerequisites (1h)](#️-prerequisites-1h)
  - [📝 Steps (2h)](#-steps-2h)
    - [1. 📥 Extract Data (1h)](#1--extract-data-1h)
    - [2. 📤 Load Data (30m)](#2--load-data-30m)
    - [3. ⚡ Transform Data (15m)](#3--transform-data-15m)
    - [4. 🚀 Run the Pipeline (15m)](#4--run-the-pipeline-15m)
  - [✨ Expected Output](#-expected-output)

## 🎯 Objective

This lab guides you through building an ELT (Extract, Load, Transform) pipeline using Python, DuckDB, and Parquet files. The goal is to extract Pokémon data from an API, store it in Parquet files, load it into a database, and perform basic transformations.

> **Data Sources:**
> 
> We'll use the [PokeAPI](https://pokeapi.co/), a free > RESTful API providing comprehensive Pokémon data:
> 
> - Endpoint: `https://pokeapi.co/api/v2/pokemon`
> - Rate limit: 100 requests per minute
> - No authentication required
> - Returns detailed information about Pokémon including:
>   - Basic stats (ID, name)
>   - Characteristics (height, weight)
>   - Abilities and types
>   - Base stats
> 
> The API supports pagination through offset/limit parameters, making it ideal > for incremental data extraction.

## 🛠️ Prerequisites (1h)

Before starting, ensure you have the following:

- Install IDE (VSCode, recommended) and Git [see here](../setup/0_install_vscode_n_git.md)
- Create your own GitHub repo [see here](../setup/1_setup_github_repo.md)
- Install Python 3.10+ [see here](../setup/2_install_python.md)
- Activate your virtual environment [see here](../setup/3_activate_virtual_environment.md)
- Optional, getting started with Duck DB, focusing on [Python](https://duckdb.org/docs/stable/clients/python/overview.html) and [CLI](https://duckdb.org/docs/stable/clients/cli/overview)

In the activated virtual environment, start install the Python packages:

- `requests` for API calls
- `pandas` & `pyarrow` for data manipulation
- `duckdb` for database operations

Using:
``sh
pip install -r requirements.txt
```

<details>
<summary>Our `requirements.txt` should contain:</summary>

```txt
requests
pandas
pyarrow
duckdb
```

</details>

## 📝 Steps (2h)

Firstly, let's create a new Python file named `lab-m1w1.py`

### 1. 📥 Extract Data (1h)

Create a new function name `extract_data`:

```python
def extract_data:
  # your code here
```

Extract Pokémon data from the PokeAPI with support for incremental extraction. Data is stored in Parquet files:

- Fetch Pokémon data in batches based on an offset.
- Save results into a Parquet file.
- Return the file path and updated offset.

### 2. 📤 Load Data (30m)

Create a new function name `load_data`:

```python
def load_data:
  # your code here
```

Load extracted data from Parquet files into DuckDB:

- Create a `pokedex` table if it doesn't exist.
- Read Parquet files and insert data into DuckDB.
- Maintain a `metadata` table to track incremental extraction.

### 3. ⚡ Transform Data (15m)

Create a new function name `transform_data`:

```python
def transform_data:
  # your code here
```

Perform aggregations and save transformed data:

- Count total Pokémon.
- Identify the first and last Pokémon ID.
- Store results in a `pokemon_stats` table.

### 4. 🚀 Run the Pipeline (15m)

Create a `main` function to execute the pipeline:

```python
def main():
    extract_data(...)
    load_data(...)
    transform_data(...)

if __name__ == "__main__":
    main()
```

Execute the full ELT pipeline:

- Extract data twice to generate two Parquet files.
- Load both files into DuckDB.
- Transform the data.

Run the script:

```sh
python lab-m1w1.py
```

## ✨ Expected Output

- A Github repository created with a python file for this ELT pipeline
- Extracted data successfully from PokeAPI
  - Simple level: Extract 1 time with limited size of output
  - Advanced level: 2 incremental extracts limited size of output and offsets
- Loaded data into DuckDB successfully with a simple metadata (e.g. last extracted pokemon id)
- Transformed summary statistics into a new table in DuckDB
- _Bonus_, Verify your data using DuckDB UI / CLI:
  1. Install DuckDB CLI ([Windows](https://duckdb.org/docs/installation/index?version=latest&environment=cli&installer=binary&platform=win) / [macOS](https://duckdb.org/docs/installation/index?version=latest&environment=cli&installer=binary&platform=macos))
  2. Open your database: `duckdb pokemon.db`
  3. Basic commands:
     ```sql
     .tables                           -- List all tables
     SELECT * FROM pokedex LIMIT 5;    -- View sample data
     SELECT * FROM pokemon_stats;      -- Check transformation results
     ```
- _Bonus_, Printing/logging the result (for debugging purpose) with emojies 🎉 🦆 🥇
- _Bonus_, Documentation, add docstring for each functions
- _Bonus_, Linting, use `black` to format your code
- _Bonus_, Package management, use `uv` to replace `requirements.txt`
