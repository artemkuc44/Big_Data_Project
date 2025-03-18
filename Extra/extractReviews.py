import csv
import time

def extract_reviews(csv_path, txt_path, limit=None):
    """Extract reviews from CSV to a text file"""
    start_time = time.time()
    count = 0
    
    with open(csv_path, 'r', encoding='utf-8') as csvfile, \
         open(txt_path, 'w', encoding='utf-8') as txtfile:
        
        reader = csv.reader(csvfile)
        next(reader)  # Skip header
        
        for row in reader:
            if len(row) > 9 and row[9]:
                txtfile.write(row[9].strip() + '\n')
                count += 1
                
                if limit and count >= limit:
                    break
    
    end_time = time.time()
    print(f"Extracted {count} reviews in {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    csv_path = "C:/Users/akuca/Desktop/bdProject/books_rating_clean.csv"
    txt_path = "C:/Users/akuca/Desktop/bdProject/reviews.txt"
    limit = None  # Set to None for all reviews
    
    extract_reviews(csv_path, txt_path, limit)