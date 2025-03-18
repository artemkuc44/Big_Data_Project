import findspark
findspark.init()

from pyspark.sql import SparkSession
import time
import re
from operator import add
import csv
from itertools import islice

# Create SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("WordCountChunked") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

def process_chunk(chunk_data, chunk_size):
    """Process a chunk of data with Spark"""
    # Create RDD from the chunk
    rdd = sc.parallelize(chunk_data)
    
    # Map: Extract words from reviews
    words = rdd.flatMap(lambda review: re.findall(r'\b\w+\b', review.lower()))
    
    # Reduce: Count words
    counts = words.map(lambda word: (word, 1)).reduceByKey(add)
    
    return counts

def chunked_wordcount(file_path, chunk_size=10000, total_limit=1000000):
    """Process the file in manageable chunks"""
    start_time = time.time()
    
    # Read and extract reviews directly using Python's CSV reader
    reviews = []
    review_count = 0
    
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header
        
        for row in reader:
            if len(row) > 9:
                review = row[9].strip()
                if review:
                    reviews.append(review)
                    review_count += 1
                    
                    if review_count >= total_limit:
                        break
    
    # Process in chunks
    all_counts = None
    for i in range(0, len(reviews), chunk_size):
        chunk = reviews[i:i+chunk_size]
        print(f"Processing chunk {i//chunk_size + 1} of {(len(reviews)-1)//chunk_size + 1}")
        
        # Get counts for this chunk
        chunk_counts = process_chunk(chunk, chunk_size)
        
        # Merge with existing counts
        if all_counts is None:
            all_counts = chunk_counts
        else:
            all_counts = all_counts.union(chunk_counts).reduceByKey(add)
    
    # Get top 10 words
    top_10 = all_counts.takeOrdered(10, key=lambda x: -x[1])
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    return execution_time, top_10

file_path = "C:/Users/akuca/Desktop/bdProject/books_rating_clean.csv"
chunk_size = 5000
total_limit = 100000  # Start with smaller limit for testing

print(f"Running chunked MapReduce with {chunk_size} chunk size for up to {total_limit} reviews...")
try:
    mr_time, mr_words = chunked_wordcount(file_path, chunk_size, total_limit)
    print(f"\nChunked MapReduce Results ({total_limit} reviews):")
    print(f"Execution time: {mr_time:.2f} seconds")
    print("Top 10 words:")
    for word, count in mr_words:
        print(f"{word}: {count}")
except Exception as e:
    import traceback
    print(f"Error occurred: {str(e)}")
    print(traceback.format_exc())
finally:
    spark.stop()