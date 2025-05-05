import re

def fix_app_py():
    # Read the file content
    with open('data_processor/app.py', 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Look at the specific area around line 1836
    print(f"Examining lines 1830-1860...")
    
    # Print the lines in the area of interest
    for i in range(1830, min(1860, len(lines))):
        print(f"Line {i}: {lines[i-1].rstrip()}")
    
    # Parse the lines to find the problem
    problem_area = ''.join(lines[1830:1855])
    
    # Look for quotes in this area
    triple_quotes = [m.start() for m in re.finditer(r'"""', problem_area)]
    print(f"Found {len(triple_quotes)} triple quotes in the problem area")
    
    # Look for SQL queries in this area
    sql_starts = [i for i, line in enumerate(lines[1830:1855], 1830) if "cursor.execute(" in line]
    print(f"SQL queries start at lines: {sql_starts}")
    
    # Check if there are any unbalanced quotes
    for start_line in sql_starts:
        print(f"Examining SQL query starting at line {start_line}")
        # Find the opening triple quote
        i = start_line
        while i < len(lines) and '"""' not in lines[i-1]:
            i += 1
        
        if i < len(lines) and '"""' in lines[i-1]:
            open_line = i
            print(f"  Opening triple quote found at line {open_line}")
            
            # Find the closing triple quote
            while i < len(lines) and not (i > open_line and '"""' in lines[i-1]):
                i += 1
            
            if i < len(lines) and '"""' in lines[i-1]:
                close_line = i
                print(f"  Closing triple quote found at line {close_line}")
            else:
                print(f"  No closing triple quote found for query starting at line {start_line}!")
                
                # Fix this by adding a closing triple quote
                indent_match = re.match(r'^(\s*)', lines[open_line-1])
                indentation = indent_match.group(1) if indent_match else '        '
                
                # Find the best place to add the closing quote - after the SQL query
                # Look for a line that likely ends the SQL query (usually before the next line of Python code)
                for j in range(open_line, min(open_line + 20, len(lines))):
                    if re.match(r'^\s*\w+', lines[j-1]) and not lines[j-1].strip().startswith('--'):
                        # This appears to be a Python line, so add the quote before it
                        fixed_lines = lines[:j-1] + [f"{indentation}\"\"\""] + lines[j-1:]
                        
                        # Write fixed content back
                        with open('data_processor/app.py', 'w', encoding='utf-8') as f:
                            f.writelines(fixed_lines)
                        print(f"Fixed unterminated triple quote by adding closing quote before line {j}")
                        return True
                
                # If we didn't find a clear ending, just add before the next non-empty line
                for j in range(open_line + 1, min(open_line + 20, len(lines))):
                    if lines[j-1].strip() and not lines[j-1].strip().startswith('"'):
                        fixed_lines = lines[:j-1] + [f"{indentation}\"\"\"\n"] + lines[j-1:]
                        
                        # Write fixed content back
                        with open('data_processor/app.py', 'w', encoding='utf-8') as f:
                            f.writelines(fixed_lines)
                        print(f"Fixed unterminated triple quote by adding closing quote before line {j}")
                        return True
    
    return False

if __name__ == "__main__":
    fix_app_py() 