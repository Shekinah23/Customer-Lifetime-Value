import pandas as pd
import sqlite3
import os

def create_loan_tables(conn):
    """Create tables for loan data"""
    cursor = conn.cursor()
    
    # Drop existing tables if they exist
    cursor.execute("DROP TABLE IF EXISTS loan_payments")
    cursor.execute("DROP TABLE IF EXISTS loan_balance")
    cursor.execute("DROP TABLE IF EXISTS loan_info")
    
    # Create tables with indexes
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS loan_info (
            proj_id INTEGER,
            loan_type TEXT,
            original_amount REAL,
            interest_rate REAL,
            start_date TEXT,
            maturity_date TEXT,
            monthly_payment REAL,
            origination_date TEXT,
            ACC_NUM INTEGER,
            currency TEXT,
            UNIQUE(proj_id, loan_type, origination_date)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS loan_balance (
            proj_id INTEGER,
            loan_type TEXT,
            origination_date TEXT,
            outstanding_balance REAL,
            days_past_due INTEGER,
            UNIQUE(proj_id, loan_type, origination_date),
            FOREIGN KEY (proj_id, loan_type, origination_date) 
                REFERENCES loan_info(proj_id, loan_type, origination_date)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS loan_payments (
            proj_id INTEGER,
            loan_type TEXT,
            origination_date TEXT,
            payment_count INTEGER,
            UNIQUE(proj_id, loan_type, origination_date),
            FOREIGN KEY (proj_id, loan_type, origination_date) 
                REFERENCES loan_info(proj_id, loan_type, origination_date)
        )
    """)
    
    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_loan_type ON loan_info(loan_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_start_date ON loan_info(start_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_dpd ON loan_balance(days_past_due)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_currency ON loan_info(currency)")
    
    conn.commit()

def import_loan_data():
    """Import loan data from Excel files into SQLite"""
    loan_data_dir = "data_processor/datasets/loan data"
    db_path = "data_processor/banking_data.db"
    
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    create_loan_tables(conn)
    
    try:
        # First read headers to check column names
        proj_info_headers = pd.read_excel(os.path.join(loan_data_dir, "M_PROJ_INFO.xlsx"), nrows=0)
        fin_bal_headers = pd.read_excel(os.path.join(loan_data_dir, "M_PROJ_FINBAL.xlsx"), nrows=0)
        repmt_headers = pd.read_excel(os.path.join(loan_data_dir, "M_REPMT_SCHD.xlsx"), nrows=0)
        
        print("Project Info Headers:", list(proj_info_headers.columns))
        print("Financial Balance Headers:", list(fin_bal_headers.columns))
        print("Repayment Headers:", list(repmt_headers.columns))
        
        # Read Excel files with verified column names and print sample data
        proj_info_df = pd.read_excel(
            os.path.join(loan_data_dir, "M_PROJ_INFO.xlsx"),
            usecols=['MPI_PROJ_ID', 'MPI_LOAN_TYPE', 'MPI_PROJ_COST', 'MPI_NET_INT_RT', 
                     'MPI_ST_DT', 'MPI_MAT_DT', 'MPI_EMI', 'MPI_DISB_DT', 'MPI_BANK_ACC_NO', 'MPI_CRLT_CON_CCY']
        )
        print("\nProject Info Sample:")
        print(proj_info_df.head())
        print("\nProject Info Data Types:")
        print(proj_info_df.dtypes)
        
        fin_bal_df = pd.read_excel(
            os.path.join(loan_data_dir, "M_PROJ_FINBAL.xlsx"),
            usecols=['MPF_PROJ_ID', 'MPF_TOT_DISB_AMT', 'MPF_DPD']
        )
        print("\nFinancial Balance Sample:")
        print(fin_bal_df.head())
        print("\nFinancial Balance Data Types:")
        print(fin_bal_df.dtypes)
        
        repmt_df = pd.read_excel(
            os.path.join(loan_data_dir, "M_REPMT_SCHD.xlsx"),
            usecols=['MRS_PROJ_ID']
        )
        print("\nRepayment Sample:")
        print(repmt_df.head())
        print("\nRepayment Data Types:")
        print(repmt_df.dtypes)
        
        # Process and insert loan info
        loan_info_data = []
        print(f"Processing {len(proj_info_df)} loan records...")
        for _, row in proj_info_df.iterrows():
            try:
                proj_id = int(row['MPI_PROJ_ID']) if pd.notnull(row['MPI_PROJ_ID']) else 0
                loan_type = str(row['MPI_LOAN_TYPE']) if pd.notnull(row['MPI_LOAN_TYPE']) else ''
                proj_cost = float(row['MPI_PROJ_COST']) if pd.notnull(row['MPI_PROJ_COST']) else 0.0
                int_rate = float(row['MPI_NET_INT_RT']) if pd.notnull(row['MPI_NET_INT_RT']) else 0.0
                emi = float(row['MPI_EMI']) if pd.notnull(row['MPI_EMI']) else 0.0
                
                # Get currency value, default to 'USD' if missing
                currency = str(row['MPI_CRLT_CON_CCY']) if pd.notnull(row['MPI_CRLT_CON_CCY']) else 'USD'
                
                # Convert dates safely
                start_date = row['MPI_ST_DT'].strftime('%Y-%m-%d') if pd.notnull(row['MPI_ST_DT']) else None
                mat_date = row['MPI_MAT_DT'].strftime('%Y-%m-%d') if pd.notnull(row['MPI_MAT_DT']) else None
                disb_date = row['MPI_DISB_DT'].strftime('%Y-%m-%d') if pd.notnull(row['MPI_DISB_DT']) else None
                
                acc_num = int(row['MPI_BANK_ACC_NO']) if pd.notnull(row['MPI_BANK_ACC_NO']) else 0
                
                loan_info_data.append((
                    proj_id,
                    loan_type,
                    proj_cost,
                    int_rate,
                    start_date,
                    mat_date,
                    emi,
                    disb_date if disb_date else 'None',  # Use 'None' as string for NULL dates
                    acc_num,
                    currency
                ))
            except Exception as e:
                print(f"Error processing loan row {_}: {str(e)}")
        
        # Process and insert loan balances
        loan_balance_data = []
        print(f"Processing {len(fin_bal_df)} balance records...")
        for _, row in fin_bal_df.iterrows():
            try:
                # Use the actual project ID
                proj_id = int(row['MPF_PROJ_ID']) if pd.notnull(row['MPF_PROJ_ID']) else 0
                disb_amt = float(row['MPF_TOT_DISB_AMT']) if pd.notnull(row['MPF_TOT_DISB_AMT']) else 0.0
                dpd = int(row['MPF_DPD']) if pd.notnull(row['MPF_DPD']) else 0
                
                # Get loan info for this project ID
                loan_info_rows = proj_info_df[proj_info_df['MPI_PROJ_ID'] == proj_id]
                if not loan_info_rows.empty:
                    # Calculate balance per loan based on total loans for this project
                    num_loans = len(loan_info_rows)
                    balance_per_loan = disb_amt / num_loans if num_loans > 0 else 0
                    
                    # Add a balance record for each loan of this project
                    for _, loan_info in loan_info_rows.iterrows():
                        loan_type = str(loan_info['MPI_LOAN_TYPE']) if pd.notnull(loan_info['MPI_LOAN_TYPE']) else ''
                        disb_date = loan_info['MPI_DISB_DT'].strftime('%Y-%m-%d') if pd.notnull(loan_info['MPI_DISB_DT']) else 'None'
                        
                        loan_balance_data.append((
                            proj_id,
                            loan_type,
                            disb_date,
                            balance_per_loan,  # Distribute balance evenly
                            dpd
                        ))
            except Exception as e:
                print(f"Error processing balance row {_}: {str(e)}")
        
        # Process and insert payment counts
        print(f"Processing {len(repmt_df)} payment records...")
        payment_counts = repmt_df['MRS_PROJ_ID'].value_counts()
        loan_payments_data = []
        for proj_id, count in payment_counts.items():
            try:
                # Use the actual project ID
                if pd.notnull(proj_id):
                    # Get loan info for this project ID
                    loan_info_rows = proj_info_df[proj_info_df['MPI_PROJ_ID'] == proj_id]
                    if not loan_info_rows.empty:
                        # Calculate payments per loan based on total loans for this project
                        num_loans = len(loan_info_rows)
                        payments_per_loan = count / num_loans if num_loans > 0 else 0
                        
                        # Add a payment record for each loan of this project
                        for _, loan_info in loan_info_rows.iterrows():
                            loan_type = str(loan_info['MPI_LOAN_TYPE']) if pd.notnull(loan_info['MPI_LOAN_TYPE']) else ''
                            disb_date = loan_info['MPI_DISB_DT'].strftime('%Y-%m-%d') if pd.notnull(loan_info['MPI_DISB_DT']) else 'None'
                            
                            loan_payments_data.append((
                                int(proj_id),
                                loan_type,
                                disb_date,
                                int(payments_per_loan)  # Distribute payments evenly
                            ))
            except Exception as e:
                print(f"Error processing payment count for project {proj_id}: {str(e)}")
        
        # Batch insert data with progress reporting
        cursor = conn.cursor()
        
        print(f"Inserting {len(loan_info_data)} loan records...")
        cursor.executemany(
            "INSERT OR REPLACE INTO loan_info VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (proj_id, loan_type, origination_date) DO UPDATE SET original_amount=excluded.original_amount, interest_rate=excluded.interest_rate, start_date=excluded.start_date, maturity_date=excluded.maturity_date, monthly_payment=excluded.monthly_payment, ACC_NUM=excluded.ACC_NUM, currency=excluded.currency",
            loan_info_data
        )
        
        print(f"Inserting {len(loan_balance_data)} balance records...")
        cursor.executemany(
            "INSERT OR REPLACE INTO loan_balance VALUES (?, ?, ?, ?, ?) ON CONFLICT (proj_id, loan_type, origination_date) DO UPDATE SET outstanding_balance=excluded.outstanding_balance, days_past_due=excluded.days_past_due",
            loan_balance_data
        )
        
        print(f"Inserting {len(loan_payments_data)} payment records...")
        cursor.executemany(
            "INSERT OR REPLACE INTO loan_payments VALUES (?, ?, ?, ?) ON CONFLICT (proj_id, loan_type, origination_date) DO UPDATE SET payment_count=excluded.payment_count",
            loan_payments_data
        )
        
        conn.commit()
        
        # Verify data import
        cursor.execute("SELECT COUNT(*) FROM loan_info")
        loan_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM loan_balance")
        balance_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM loan_payments")
        payment_count = cursor.fetchone()[0]
        
        print(f"\nImport Summary:")
        print(f"Loan records: {loan_count}")
        print(f"Balance records: {balance_count}")
        print(f"Payment records: {payment_count}")
        print("\nLoan data imported successfully")
        
    except Exception as e:
        print(f"Error importing loan data: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    import_loan_data()
