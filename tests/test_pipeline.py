import pytest
import pandas as pd
from etl.pipeline import extract_data, transform_data, load_data, run_pipeline

def test_extract_data():
    """Test the extract function"""
    df = extract_data()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5
    assert list(df.columns) == ['id', 'name', 'value']

def test_transform_data():
    """Test the transform function"""
    df = extract_data()
    transformed = transform_data(df)
    assert 'value_doubled' in transformed.columns
    assert 'name_upper' in transformed.columns
    assert transformed['value_doubled'].iloc[0] == 200
    assert transformed['name_upper'].iloc[0] == 'ALICE'

def test_load_data():
    """Test the load function"""
    df = extract_data()
    result = load_data(df)
    assert result['status'] == 'success'
    assert result['records'] == 5
    assert result['total_value'] == 1000

def test_run_pipeline():
    """Test the complete pipeline"""
    result = run_pipeline()
    assert result['status'] == 'success'
    assert result['records'] == 5
