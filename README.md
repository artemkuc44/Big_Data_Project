# Book Review & Author Sentiment Analysis

A Big Data project analyzing sentiment in Amazon book reviews and correlating findings with Goodreads author profiles using Apache Spark and Scala.

## Project Overview

This project uses data processing and analysis techniques to answer the question: "Do 'Good Reads' authors earn their reputation through Amazon reviews?"

We analyze sentiment in Amazon book reviews, classify authors based on this sentiment, and compare the results with Goodreads author profiles to validate if highly-rated authors actually receive positive reviews from readers.

## Datasets

- **Amazon Book Reviews**: 3 million entries (2.3GB)
- **Amazon Book Data**: 212,400 entries (180MB)
- **Goodreads Author Profiles**: 209,517 entries (113MB)

## Directory Structure

```
├── ScalaObjectiveCode/
│   ├── src/main/scala/com/wordcount/
│   │   ├── Sequential Implementation
│   │   │   ├── SeqAuthorSentimentClassifier.scala
│   │   │   ├── SeqGoodReadAuthorAnalyzer.scala
│   │   │   ├── WordCount.scala
│   │   ├── Parallel Implementation
│   │   │   ├── AuthorSentimentClassifier.scala
│   │   │   ├── GoodReadAuthorAnalyzer.scala
│   │   │   ├── SparkWordCount.scala
│   │   ├── Utility Files
│   │   │   ├── CompareAuthorLists.scala
│   │   │   ├── authorsDataExtractSortAuthors.scala
│   │   │   ├── booksDataExtractSortAuthors.scala
│   ├── build.sbt
├── Extra/
│   ├── Various Python scripts for data exploration
├── results.txt
└── clean.py
```

## Core Components

### Sequential Implementation

- **SeqAuthorSentimentClassifier.scala**: Processes book reviews sequentially to classify authors based on sentiment
- **SeqGoodReadAuthorAnalyzer.scala**: Matches classified authors with Goodreads dataset
- **WordCount.scala**: Sequential word count benchmark (takes ~21 minutes)

### Parallel Implementation (Spark)

- **AuthorSentimentClassifier.scala**: Parallelized version using Spark RDDs for sentiment analysis
- **GoodReadAuthorAnalyzer.scala**: Parallelized version for matching authors with Goodreads data
- **SparkWordCount.scala**: Parallel word count benchmark (takes ~31 seconds)

### Utility Files

- **CompareAuthorLists.scala**: Compares matched authors between datasets
- **authorsDataExtractSortAuthors.scala**: Extracts and sorts author data from Goodreads
- **booksDataExtractSortAuthors.scala**: Extracts and sorts author data from Amazon books

## Performance Comparison

| Implementation | Sequential | Parallel (Spark) | Improvement |
|----------------|------------|------------------|-------------|
| Word Count     | 1292.6s    | 31.0s            | 41.7x       |
| Sentiment Analysis | 80.6s  | 25.6s            | 3.2x        |
| Author Matching   | 4.0s    | 2.4s             | 1.7x        |

## Key Findings

- 37,143 authors were found in both Amazon and Goodreads datasets
- Among matched authors:
  - 33,673 (90.7%) classified as "good" based on Amazon reviews
  - 707 (1.9%) classified as "bad" based on Amazon reviews
  - 2,763 (7.4%) classified as "neutral" based on Amazon reviews
- Only 17.63% of authors classified as "bad" in our analysis were found in the Goodreads dataset
- The results indicate that Goodreads authors generally receive positive sentiment in Amazon reviews

## How to Run

1. Ensure you have Scala and Spark installed
2. Compile the project using SBT:
   ```
   cd ScalaObjectiveCode
   sbt compile
   ```
3. Run the sequential or parallel implementations:
   ```
   sbt "runMain com.wordcount.SeqAuthorSentimentClassifier"
   sbt "runMain com.wordcount.AuthorSentimentClassifier"
   ```

## Extra Files

The "Extra" directory contains Python scripts used for initial data exploration and testing before implementing the Scala solutions.

## Authors

- Artjoms Kucajevs (22385231)
- Shlok Patel (22715709)
