import pandas as pd
import re
import ast

def clean_csv(input_file, output_file, file_type):
    print(f"Cleaning {input_file}...")
    
    # Load CSV with appropriate encoding
    try:
        df = pd.read_csv(input_file, encoding='utf-8')
    except:
        try:
            df = pd.read_csv(input_file, encoding='latin1')
        except:
            df = pd.read_csv(input_file, encoding='ISO-8859-1', on_bad_lines='skip')
    
    # Common cleaning for all files
    for col in df.columns:
        if df[col].dtype == 'object':
            # Replace newlines and multiple spaces
            df[col] = df[col].str.replace('\n', ' ').str.replace('\r', ' ').str.replace('\t', ' ')
            df[col] = df[col].str.replace(r'\s+', ' ', regex=True).str.strip()
    
    # File-specific cleaning
    if file_type == 'books_rating':
        # Clean review text - handle quotes and special characters
        if 'review/text' in df.columns:
            df['review/text'] = df['review/text'].str.replace('"', "'")
        
        # Convert scores to proper floats
        if 'review/score' in df.columns:
            df['review/score'] = pd.to_numeric(df['review/score'], errors='coerce')
    
    elif file_type == 'books_data':
        # Fix authors list formatting
        if 'authors' in df.columns:
            df['authors'] = df['authors'].apply(lambda x: clean_authors_list(x) if pd.notna(x) else x)
        
        # Fix categories list formatting
        if 'categories' in df.columns:
            df['categories'] = df['categories'].apply(lambda x: clean_authors_list(x) if pd.notna(x) else x)
    
    elif file_type == 'authors_data':
        # Standardize author names
        if 'name' in df.columns:
            df['name'] = df['name'].str.strip().str.replace(r'\s+', ' ', regex=True)
        
        # Clean HTML tags from about field
        if 'about' in df.columns:
            df['about'] = df['about'].str.replace(r'<br\s*/?>', ' ', regex=True)
            df['about'] = df['about'].str.replace(r'<.*?>', '', regex=True)
    
    # Write cleaned file
    df.to_csv(output_file, index=False, quoting=1)  # quoting=1 means quote all strings
    print(f"Saved cleaned file to {output_file}")
    
    return df

def clean_authors_list(authors_str):
    # Convert string representation of list to proper format
    try:
        if isinstance(authors_str, str):
            if '[' in authors_str and ']' in authors_str:
                # Try to safely evaluate the string as a Python list
                try:
                    authors_list = ast.literal_eval(authors_str)
                    return str(authors_list)
                except:
                    # Clean manually if eval fails
                    authors_str = authors_str.strip('[]')
                    authors = [a.strip().strip("'\"") for a in authors_str.split(',')]
                    return str(authors)
    except:
        pass
    return authors_str

# Clean all three files
clean_csv("C:\\Users\\akuca\\Desktop\\bdProject\\books_rating.csv", 
          "C:\\Users\\akuca\\Desktop\\bdProject\\books_rating_clean.csv", 
          "books_rating")

clean_csv("C:\\Users\\akuca\\Desktop\\bdProject\\books_data.csv", 
          "C:\\Users\\akuca\\Desktop\\bdProject\\books_data_clean.csv", 
          "books_data")

clean_csv("C:\\Users\\akuca\\Desktop\\bdProject\\authors_data.csv", 
          "C:\\Users\\akuca\\Desktop\\bdProject\\authors_data_clean.csv", 
          "authors_data")