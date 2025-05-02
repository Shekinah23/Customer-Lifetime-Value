import pandas as pd
import os

def analyze_excel_file(file_path):
    print(f"\nAnalyzing {os.path.basename(file_path)}:")
    print("-" * 50)
    
    # Read the Excel file
    df = pd.read_excel(file_path)
    
    # Basic information
    print(f"Number of rows: {len(df)}")
    print(f"Number of columns: {len(df.columns)}")
    print("\nColumns:")
    for col in df.columns:
        # Get sample non-null value
        sample_value = df[col].dropna().iloc[0] if not df[col].dropna().empty else "No non-null values"
        print(f"- {col}")
        print(f"  Data type: {df[col].dtype}")
        print(f"  Sample value: {sample_value}")
        print(f"  Null values: {df[col].isnull().sum()}")
        print()

def main():
    loan_data_dir = "data_processor/datasets/loan data"
    excel_files = [
        "M_PROJ_FINBAL.xlsx",
        "M_PROJ_INFO.xlsx",
        "M_REPMT_SCHD.xlsx",
        "P_PRD.xlsx"
    ]
    
    print("Loan Data Analysis")
    print("=" * 50)
    
    for file in excel_files:
        file_path = os.path.join(loan_data_dir, file)
        try:
            analyze_excel_file(file_path)
        except Exception as e:
            print(f"Error analyzing {file}: {str(e)}")

if __name__ == "__main__":
    main()
