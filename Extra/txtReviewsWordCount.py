import findspark
findspark.init()

from pyspark.sql import SparkSession
import time
import re
from operator import add

# Create SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("TextReviewWordCount") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

def text_wordcount(file_path):
    """Process review text file with Spark"""
    start_time = time.time()
    
    # Load text file directly - each line is a review
    reviews = sc.textFile(file_path)
    
    # Standard MapReduce wordcount:
    # 1. Map: Tokenize each review into words
    # 2. Reduce: Count occurrences of each word
    word_counts = reviews.flatMap(lambda review: re.findall(r'\b\w+\b', review.lower())) \
                        .map(lambda word: (word, 1)) \
                        .reduceByKey(add)
    
    # Get top words
    top_words = word_counts.takeOrdered(10, key=lambda x: -x[1])
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    return execution_time, top_words

if __name__ == "__main__":
    file_path = "C:/Users/akuca/Desktop/bdProject/reviews.txt"
    
    print("Running MapReduce on text file...")
    try:
        mr_time, mr_words = text_wordcount(file_path)
        print(f"\nMapReduce Results:")
        print(f"Execution time: {mr_time:.2f} seconds")
        print("Top 10 words:")
        for word, count in mr_words:
            print(f"{word}: {count}")
    except Exception as e:
        print(f"Error occurred: {str(e)}")
    finally:
        spark.stop()