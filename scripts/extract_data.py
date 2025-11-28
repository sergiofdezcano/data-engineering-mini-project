"""
Extract cryptocurrency data from CoinGecko API
"""
import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests


def extract_crypto_data(output_dir: str = "/opt/airflow/data/raw") -> str:
    """
    Extract top 50 cryptocurrencies data from CoinGecko API

    Args:
        output_dir: Directory to save the extracted data

    Returns:
        str: Path to the saved file
    """
    # API endpoint
    url = "https://api.coingecko.com/api/v3/coins/markets"

    # Parameters for the API call
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 50,
        'page': 1,
        'sparkline': False,
        'price_change_percentage': '24h,7d'
    }

    print(f"Extracting data from CoinGecko API...")

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Add extraction timestamp
        df['extracted_at'] = datetime.utcnow().isoformat()

        # Create output directory if it doesn't exist
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Generate filename with timestamp
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"crypto_data_{timestamp}.json"
        filepath = os.path.join(output_dir, filename)

        # Save as JSON
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"Data extracted successfully: {len(df)} records")
        print(f"Saved to: {filepath}")

        return filepath

    except requests.exceptions.RequestException as e:
        print(f"Error extracting data: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise


def load_to_postgres(filepath: str, table_name: str = "raw_crypto_data"):
    """
    Load extracted data to PostgreSQL

    Args:
        filepath: Path to the JSON file
        table_name: Target table name
    """
    from sqlalchemy import create_engine

    # Database connection string
    conn_string = "postgresql://airflow:airflow@postgres:5432/data_warehouse"

    print(f"Loading data to PostgreSQL table: {table_name}")

    try:
        # Read JSON file
        with open(filepath, 'r') as f:
            data = json.load(f)

        df = pd.DataFrame(data)

        # Add extracted_at timestamp if not present
        if 'extracted_at' not in df.columns:
            df['extracted_at'] = datetime.utcnow().isoformat()

        # Create database engine
        engine = create_engine(conn_string)

        # Load to PostgreSQL (replace if exists)
        df.to_sql(table_name, engine, if_exists='append', index=False)

        print(f"Loaded {len(df)} records to {table_name}")

    except Exception as e:
        print(f"Error loading to PostgreSQL: {e}")
        raise


if __name__ == "__main__":
    # For testing purposes
    filepath = extract_crypto_data(output_dir="./data/raw")
    print(f"\nExtraction complete: {filepath}")
