import csv
import time

def traditional_common_authors(books_path, authors_path):
    start_time = time.time()
    
    # Extract authors from books data
    books_authors = set()
    with open(books_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        for row in reader:
            if len(row) >= 3:  # Ensure authors field exists
                authors_field = row[2]  # Authors field index
                # Clean up the authors field
                authors_field = authors_field.strip("[]'")
                authors_list = [a.strip("' ").lower() for a in authors_field.split("','")]
                for author in authors_list:
                    if author:  # Skip empty author names
                        books_authors.add(author)
    
    # Extract author names from authors data
    author_names = set()
    with open(authors_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        for row in reader:
            if len(row) >= 2:  # Ensure name field exists
                name = row[1].strip().lower()  # Name field index
                if name:  # Skip empty names
                    author_names.add(name)
    
    # Find common authors
    common_authors = books_authors.intersection(author_names)
    count = len(common_authors)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    print(f"Traditional approach results:")
    print(f"Number of authors in both datasets: {count}")
    print(f"Execution time: {execution_time:.2f} seconds")
    
    # Optional: Show some examples of common authors
    example_authors = list(common_authors)[:5] if common_authors else []
    print(f"Example common authors: {example_authors}")
    
    return execution_time, count, common_authors

# Paths to your files
books_path = "C:\\Users\\akuca\\Desktop\\bdProject\\books_data.csv"
authors_path = "C:\\Users\\akuca\\Desktop\\bdProject\\authors_data.csv"

# Run the traditional approach
trad_time, trad_count, common_authors = traditional_common_authors(books_path, authors_path)