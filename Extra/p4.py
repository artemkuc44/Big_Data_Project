import findspark
findspark.init()

from pyspark.sql import SparkSession
import time
import csv

# Create SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = spark.sparkContext

def mapreduce_findBad(file_path):
    start_time = time.time()
    
    # Load the data as an RDD
    rdd = sc.textFile(file_path)
    
    # Map phase: Parse CSV line properly and check if 10th field contains "bad"
    def check_bad_in_field10(line):
        try:
            fields = list(csv.reader([line]))[0]
            if len(fields) >= 10:
                return 1 if "bad" in fields[9].lower() else 0
            return 0
        except Exception as e:
            return 0
    
    mapped = rdd.map(check_bad_in_field10)
    
    # Reduce phase: Sum all the 1s to get the total count
    count = mapped.reduce(lambda x, y: x + y)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    print(f"MapReduce approach results:")
    print(f"Number of reviews with 'bad' in field 10: {count}")
    print(f"Execution time: {execution_time:.2f} seconds")
    
    return execution_time, count



# Run the MapReduce approach
file_path = "C:\\Users\\akuca\\Desktop\\bdProject\\books_rating.csv"

#mr_time, mr_count = mapreduce_findBad(file_path)

# Clean up
spark.stop()