import csv

def count_csv_entries(file_path, has_header=True):
    """Counts the number of data rows in a CSV file.
    
    Parameters:
      file_path (str): The path to the CSV file.
      has_header (bool): Whether the CSV file contains a header row.
    
    Returns:
      int: The number of data entries (rows excluding the header if present).
    """
    count = 0
    with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for i, row in enumerate(reader):
            # If there is a header row, skip it
            if i == 0 and has_header:
                continue
            count += 1
    return count

if __name__ == '__main__':
    cleaned_files = [
        r"C:\Users\akuca\Desktop\bdProject\books_rating_clean.csv",
        r"C:\Users\akuca\Desktop\bdProject\books_data_clean.csv",
        r"C:\Users\akuca\Desktop\bdProject\authors_data_clean.csv"
    ]
    
    for file_path in cleaned_files:
        entries = count_csv_entries(file_path)
        print(f"{file_path} has {entries} entries.")
