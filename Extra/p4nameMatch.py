
import findspark
findspark.init()

from pyspark.sql import SparkSession
import time
import csv

# Create SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = spark.sparkContext

def mapreduce_common_authors(books_path, authors_path):
    start_time = time.time()
    
    # Load the data as RDDs
    books_rdd = sc.textFile(books_path)
    authors_rdd = sc.textFile(authors_path)
    
    # Skip headers
    header_books = books_rdd.first()
    header_authors = authors_rdd.first()
    books_rdd = books_rdd.filter(lambda line: line != header_books)
    authors_rdd = authors_rdd.filter(lambda line: line != header_authors)
    
    # Map phase for books: Extract author names
    def extract_book_authors(line):
        try:
            fields = list(csv.reader([line]))[0]
            if len(fields) >= 7:  # Ensure authors field exists
                authors_field = fields[2]  # Authors field index
                # Clean up the authors field
                authors_field = authors_field.strip("[]'")
                authors_list = [a.strip("' ") for a in authors_field.split("','")]
                return authors_list
            return []
        except Exception as e:
            return []
    
    # Map phase for authors: Extract author names
    def extract_author_names(line):
        try:
            fields = list(csv.reader([line]))[0]
            if len(fields) >= 2:  # Ensure name field exists
                return [fields[1].strip()]  # Name field index
            return []
        except Exception as e:
            return []
    
    # Extract all authors from both datasets
    books_authors = books_rdd.flatMap(extract_book_authors).map(lambda name: (name.lower(), 1))
    author_names = authors_rdd.flatMap(extract_author_names).map(lambda name: (name.lower(), 1))
    
    # Find common authors by joining datasets
    common_authors = books_authors.join(author_names).distinct()
    count = common_authors.count()
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    print(f"MapReduce approach results:")
    print(f"Number of authors in both datasets: {count}")
    print(f"Execution time: {execution_time:.2f} seconds")
    
    # Optional: Show some examples of common authors
    example_authors = common_authors.keys().take(5)
    print(f"Example common authors: {example_authors}")
    
    return execution_time, count

# Paths to your files
books_path = "C:\\Users\\akuca\\Desktop\\bdProject\\books_data.csv"
authors_path = "C:\\Users\\akuca\\Desktop\\bdProject\\authors_data.csv"

# Run the MapReduce approach
mr_time, mr_count = mapreduce_common_authors(books_path, authors_path)

spark.stop()