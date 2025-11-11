import pandas as pd
import os

file_path = '/home/datavista/datavista_docker/api_engine/tests/datarow-spbu-54xxxx.xlsx'

try:
    df = pd.read_excel(file_path)
    print("Columns in datarow-spbu-54xxxx.xlsx:")
    for col in df.columns:
        print(col)
except Exception as e:
    print(f"Error reading Excel file: {e}")
