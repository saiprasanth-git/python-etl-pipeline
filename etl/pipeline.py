import pandas as pd
from typing import Dict, Any

def extract_data() -> pd.DataFrame:
    """Extract data from source"""
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'value': [100, 200, 150, 300, 250]
    }
    return pd.DataFrame(data)

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform the data"""
    df['value_doubled'] = df['value'] * 2
    df['name_upper'] = df['name'].str.upper()
    return df

def load_data(df: pd.DataFrame) -> Dict[str, Any]:
    """Load data to destination"""
    return {
        'status': 'success',
        'records': len(df),
        'total_value': df['value'].sum()
    }

def run_pipeline():
    """Run the complete ETL pipeline"""
    print("Starting ETL Pipeline...")
    
    # Extract
    print("Extracting data...")
    raw_data = extract_data()
    print(f"Extracted {len(raw_data)} records")
    
    # Transform
    print("Transforming data...")
    transformed_data = transform_data(raw_data)
    print("Data transformed")
    
    # Load
    print("Loading data...")
    result = load_data(transformed_data)
    print(f"Pipeline complete: {result}")
    
    return result

if __name__ == '__main__':
    run_pipeline()
