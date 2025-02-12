import pandas as pd

def inspect_csv(file_path, filename):
    print(f"\nInspecting {filename}:")
    df = pd.read_csv(file_path, encoding='latin1', low_memory=False)
    print("\nColumns and first non-null value:")
    for col in df.columns:
        first_val = df[col].iloc[0] if not df[col].empty else None
        print(f"- {col}: {first_val}")
    print("\nSample row as dict:")
    print(df.iloc[0].to_dict())
    print("\n" + "="*50)

# Inspect all CSV files
files = {
    "corpclients": "datasets/corpclients.csv",
    "indclients": "datasets/indclients.csv"
}

for name, path in files.items():
    try:
        inspect_csv(path, name)
    except Exception as e:
        print(f"Error reading {name}: {str(e)}")
