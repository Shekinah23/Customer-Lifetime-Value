import re

def fix_app_py():
    with open('data_processor/app.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Locate the loan_currencies route where the error is occurring
    pattern = r"@app\.route\('/api/loan-currencies'\)(.*?)def get_loan_currencies\(\):(.*?)cursor\.execute\((.*?)SELECT DISTINCT(.*?)ORDER BY currency(.*?)\"\"\"(.*?)if __name__ == '__main__':"
    match = re.search(pattern, content, re.DOTALL)
    
    if match:
        print("Found the problematic route")
        # Extract the route text
        route_text = match.group(0)
        
        # Create a fixed version with proper quotes
        fixed_route = """@app.route('/api/loan-currencies')
@login_required
def get_loan_currencies():
    try:
        conn = get_db()
        if conn is None:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Get distinct currency values
        cursor.execute('''
            SELECT DISTINCT currency 
            FROM loan_info 
            WHERE currency IS NOT NULL AND currency != ''
            ORDER BY currency
        ''')
        
        currencies = [dict(row)['currency'] for row in cursor.fetchall()]
        
        # Add USD as default if not already present
        if not currencies or 'USD' not in currencies:
            currencies.insert(0, 'USD')
            
        return jsonify({'currencies': currencies})
        
    except Exception as e:
        print(f"Error fetching currency data: {str(e)}")
        return jsonify({'error': 'Failed to fetch currency data'}), 500

if __name__ == '__main__':"""
        
        # Replace the problematic code
        fixed_content = content.replace(route_text, fixed_route)
        
        # Write the fixed content back to the file
        with open('data_processor/app.py', 'w', encoding='utf-8') as f:
            f.write(fixed_content)
        
        print("Fixed the file")
        return True
    
    print("Could not locate the problematic route")
    return False

if __name__ == "__main__":
    fix_app_py() 