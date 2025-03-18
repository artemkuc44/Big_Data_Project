# import csv
# import time

# start_time = time.time()
# count = 0

# with open("C:/Users/akuca/Desktop/bdProject/books_rating.csv", 'r', encoding='utf-8') as csvfile:
#     reader = csv.DictReader(csvfile)
#     for row in reader:
#         # Assuming the relevant column is named 'review/text' or 'review_text'
#         review = row.get('review/text') 
#         if review and 'bad' in review.lower():
#             count += 1

# end_time = time.time()
# print("CSV Query Results (using csv module):")
# print("Count:", count)
# print("Execution time:", end_time - start_time, "seconds")


import csv
import time
import re
from collections import Counter
from itertools import islice

start_time = time.time()
word_count = Counter()

# Set the number of rows to process for the partial result
row_limit = 1000000

with open("C:/Users/akuca/Desktop/bdProject/books_rating.csv", 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in islice(reader, row_limit):  # Process only the first 'row_limit' rows
        review = row.get('review/text')
        if review:
            words = re.findall(r'\b\w+\b', review.lower())
            word_count.update(words)

# Get the top 10 most common words from the partial data
top_10_words = word_count.most_common(10)

end_time = time.time()

print("Top 10 Most Used Words (from partial dataset with ", row_limit, " entries):")
for word, count in top_10_words:
    print(f"{word}: {count}")
print("Execution time (partial dataset):", end_time - start_time, "seconds")
