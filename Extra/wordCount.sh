#!/bin/bash
# This script finds the top 10 most common words in review text column
start_time=$(date +%s.%N)

CSV_FILE=${1:-"books_rating_clean.csv"}
MAX_LINES=${2:-1000000}
TOP_N=${3:-10}

# Use Python for reliable CSV parsing
python3 -c "
import csv
import sys
from collections import Counter

with open('$CSV_FILE', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    word_counts = Counter()
    count = 0
    
    for row in reader:
        if count >= $MAX_LINES:
            break
        text = row['review/text'].lower() if 'review/text' in row else ''
        words = [word for word in ''.join(c if c.isalnum() else ' ' for c in text).split() 
                if word.isalpha() and len(word) > 1]
        word_counts.update(words)
        count += 1
    
    for word, count in word_counts.most_common($TOP_N):
        print(f'{count:7d} {word}')
" 2>/dev/null || echo "Error: Python script failed - check if Python is installed"

end_time=$(date +%s.%N)
runtime=$(echo "$end_time - $start_time" | bc)
echo -e "\nExecution time: $runtime seconds"