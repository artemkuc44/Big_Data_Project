package com.wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import java.io.{File, PrintWriter}

object AuthorSentimentClassifier {
  def main(args: Array[String]): Unit = {
    // Configure Spark
    val conf = new SparkConf()
      .setAppName("AuthorSentimentClassifier")
      .setMaster("local[*]")
      .set("spark.driver.memory", "4g")
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.bindAddress", "127.0.0.1")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    try {
      val startTime = System.currentTimeMillis()

      // Path to CSV files
      val booksDataFilePath = "C:/Users/akuca/Desktop/bdProject/books_data_clean.csv"
      val reviewsFilePath = "C:/Users/akuca/Desktop/bdProject/books_rating_clean.csv"
      val goodAuthorsOutputPath = "C:/Users/akuca/Desktop/bdProject/good_authors.txt"
      val badAuthorsOutputPath = "C:/Users/akuca/Desktop/bdProject/bad_authors.txt"
      val neutralAuthorsOutputPath = "C:/Users/akuca/Desktop/bdProject/neutral_authors.txt"

      val positiveWords = Set(
        "good", "great", "excellent", "amazing", "wonderful",
        "positive", "better", "best", "outstanding", "fantastic",
        "brilliant", "enjoyable", "favorite", "loved", "love",
        "impressive", "perfect", "superb", "delightful", "beautiful"
      )

      val negativeWords = Set(
        "bad", "poor", "terrible", "awful", "horrible",
        "negative", "worse", "worst", "disappointing", "disappointment",
        "mediocre", "failure", "failed", "useless", "boring",
        "waste", "dislike", "hate", "dreadful", "lousy"
      )

      // Extract ALL authors from books data using the logic from booksDataExtractSortAuthors.scala
      println("Reading books data to extract all authors...")
      val booksDataLines = sc.textFile(booksDataFilePath)
      val booksDataHeader = booksDataLines.first()

      // Get the headers to find the authors column index
      val headers = booksDataHeader.split(",").map(_.trim.replaceAll("^\"|\"$", ""))
      val authorsIndex = headers.indexWhere(_.equals("authors"))

      if (authorsIndex == -1) {
        println("Error: No 'authors' column found in the CSV file.")
        return
      }

      val titleIndex = headers.indexWhere(_.equals("Title"))
      if (titleIndex == -1) {
        println("Error: No 'Title' column found in the CSV file.")
        return
      }

      // Extract all unique authors using logic from booksDataExtractSortAuthors.scala
      val allAuthors = booksDataLines
        .filter(_ != booksDataHeader)
        .flatMap(line => {
          val fields = parseCSVLine(line)

          if (fields.length > authorsIndex) {
            val authorsField = fields(authorsIndex).trim

            // Skip if empty or just []
            if (authorsField.isEmpty || authorsField == "[]") {
              Seq.empty[String]
            } else {
              // Extract content within square brackets
              val start = authorsField.indexOf('[')
              val end = authorsField.lastIndexOf(']')

              if (start >= 0 && end > start) {
                val bracketContent = authorsField.substring(start + 1, end).trim

                // Extract authors using the custom method from booksDataExtractSortAuthors.scala
                extractAuthorsFromBracketContent(bracketContent)
              } else {
                Seq.empty[String]
              }
            }
          } else {
            Seq.empty[String]
          }
        })
        .distinct()
        .cache()

      // Also create title-to-authors mapping for sentiment analysis
      val titleToAuthors = booksDataLines
        .filter(_ != booksDataHeader)
        .map(line => {
          val fields = parseCSVLine(line)

          val title = if (fields.length > titleIndex) fields(titleIndex).trim else ""

          val authors = if (fields.length > authorsIndex) {
            val authorsField = fields(authorsIndex).trim

            // Skip if empty or just []
            if (authorsField.isEmpty || authorsField == "[]") {
              List.empty[String]
            } else {
              // Extract content within square brackets
              val start = authorsField.indexOf('[')
              val end = authorsField.lastIndexOf(']')

              if (start >= 0 && end > start) {
                val bracketContent = authorsField.substring(start + 1, end).trim

                // Extract authors using the custom method from booksDataExtractSortAuthors.scala
                extractAuthorsFromBracketContent(bracketContent)
              } else {
                List.empty[String]
              }
            }
          } else {
            List.empty[String]
          }

          (title, authors)
        })
        .filter(_._1.nonEmpty)
        .cache()

      // STEP 2: Process reviews to calculate title sentiment scores
      println("Reading reviews to calculate title sentiment scores...")
      val reviewsLines = sc.textFile(reviewsFilePath)
      val reviewsHeader = reviewsLines.first()

      // Find review text column index
      val reviewsHeaders = reviewsHeader.split(",").map(_.trim.replaceAll("^\"|\"$", ""))
      val reviewTitleIndex = reviewsHeaders.indexWhere(_.equals("Title"))
      val reviewTextIndex = reviewsHeaders.indexWhere(_.equals("review/text"))

      if (reviewTitleIndex == -1 || reviewTextIndex == -1) {
        println("Error: Required columns not found in reviews file.")
        return
      }

      // Parse reviews and extract (title, review_text) pairs
      val titleReviewPairs = reviewsLines
        .filter(_ != reviewsHeader)
        .map(line => {
          val fields = parseCSVLine(line)
          if (fields.length > Math.max(reviewTitleIndex, reviewTextIndex)) {
            (fields(reviewTitleIndex).trim, fields(reviewTextIndex).trim)
          } else {
            ("", "")
          }
        })
        .filter(pair => pair._1.nonEmpty && pair._2.nonEmpty)

      // For each title, analyze sentiment in review text
      val titleSentiment = titleReviewPairs.flatMap { case (title, reviewText) =>
        // Split review into words
        val words = reviewText.toLowerCase.split("\\W+")

        // Count positive and negative words
        val positiveCount = words.count(positiveWords.contains)
        val negativeCount = words.count(negativeWords.contains)

        // Emit pairs of (title, sentiment_type) -> count
        Seq(
          ((title, "positive"), positiveCount),
          ((title, "negative"), negativeCount)
        )
      }

      // Aggregate sentiment counts by title and calculate score
      val titleScores = titleSentiment
        .reduceByKey(_ + _)
        .map { case ((title, sentimentType), count) => (title, (sentimentType, count)) }
        .groupByKey()
        .mapValues { sentiments =>
          val sentimentMap = sentiments.toMap
          val posCount = sentimentMap.getOrElse("positive", 0)
          val negCount = sentimentMap.getOrElse("negative", 0)
          val totalWords = posCount + negCount

          if (totalWords > 0) {
            // Calculate normalized sentiment score: (positive - negative) / total
            val sentimentScore = (posCount - negCount).toDouble / totalWords.toDouble
            sentimentScore
          } else {
            0.0
          }
        }
        .cache() // Cache for joining

      // Join titles with authors and calculate author sentiment
      println("Calculating author sentiment scores...")
      val authorSentiments = titleToAuthors
        .filter(_._2.nonEmpty)
        .flatMap { case (title, authors) =>
          // Create (title, author) pairs for each author of the book
          authors.map(author => (title, author))
        }
        .join(titleScores)
        .map { case (title, (author, score)) =>
          // Output: (author, (score, count))
          (author, (score, 1))
        }

      // Aggregate sentiment scores by author
      val authorAggregatedScores = authorSentiments
        .reduceByKey { case ((scoreSum1, count1), (scoreSum2, count2)) =>
          // Sum up scores and counts
          (scoreSum1 + scoreSum2, count1 + count2)
        }
        .mapValues { case (scoreSum, bookCount) =>
          // Calculate average score
          val avgScore = if (bookCount > 0) scoreSum / bookCount else 0.0
          (avgScore, bookCount)
        }

      // Slassify all authors (including those with no reviews)
      println("Classifying all authors...")

      // Create RDD of all authors with their sentiment scores (if available)
      val allAuthorsWithSentiment = allAuthors
        .map(author => (author, (0.0, 0))) // Default is neutral
        .leftOuterJoin(authorAggregatedScores)
        .map { case (author, (default, optionalScore)) =>
          val (score, bookCount) = optionalScore.getOrElse(default)
          (author, score, bookCount)
        }
        .cache()

      // Classify authors into good, bad, neutral categories
      val goodAuthors = allAuthorsWithSentiment
        .filter { case (_, score, _) => score > 0.0 }
        .map(_._1)
        .collect()

      val badAuthors = allAuthorsWithSentiment
        .filter { case (_, score, _) => score < 0.0 }
        .map(_._1)
        .collect()

      val neutralAuthors = allAuthorsWithSentiment
        .filter { case (_, score, _) => score == 0.0 }
        .map(_._1)
        .collect()

      // Write results trgo files
      println("Writing results to files...")

      // Write good authors list
      val goodAuthorsWriter = new PrintWriter(new File(goodAuthorsOutputPath))
      try {
        goodAuthors.foreach(author => goodAuthorsWriter.println(author))
      } finally {
        goodAuthorsWriter.close()
      }

      // Write bad authors list
      val badAuthorsWriter = new PrintWriter(new File(badAuthorsOutputPath))
      try {
        badAuthors.foreach(author => badAuthorsWriter.println(author))
      } finally {
        badAuthorsWriter.close()
      }

      // Write neutral authors list
      val neutralAuthorsWriter = new PrintWriter(new File(neutralAuthorsOutputPath))
      try {
        neutralAuthors.foreach(author => neutralAuthorsWriter.println(author))
      } finally {
        neutralAuthorsWriter.close()
      }

      val executionTime = (System.currentTimeMillis() - startTime) / 1000.0

      // Print summary statistics
      println("\nAuthor Sentiment Classification Results:")
      println("------------------------------------------")
      println(s"Total unique authors: ${allAuthors.count()}")
      println(s"Authors with sentiment data: ${authorAggregatedScores.count()}")
      println(s"Authors classified as good (score > 0.0): ${goodAuthors.length}")
      println(s"Authors classified as bad (score < 0.0): ${badAuthors.length}")
      println(s"Authors classified as neutral (score = 0.0 or no reviews): ${neutralAuthors.length}")
      println(s"Results saved to: $goodAuthorsOutputPath, $badAuthorsOutputPath, and $neutralAuthorsOutputPath")
      println(f"Execution time: $executionTime%.2f seconds")

    } finally {
      sc.stop()
    }
  }

  // Custom method to extract authors from the brackfet content implementation from booksDataExtractSortAuthors.scala
  def extractAuthorsFromBracketContent(content: String): List[String] = {
    if (content.isEmpty) return List.empty

    // State variables for parsing
    val result = scala.collection.mutable.ListBuffer[String]()
    var currentAuthor = new StringBuilder()
    var inSingleQuotes = false
    var inDoubleQuotes = false

    // Process each character
    for (i <- 0 until content.length) {
      val c = content(i)

      c match {
        case '\'' =>
          inSingleQuotes = !inSingleQuotes
        // Don't include the quote in the result

        case '"' =>
          inDoubleQuotes = !inDoubleQuotes
        // Don't include the quote in the result

        case ',' if !inSingleQuotes && !inDoubleQuotes =>
          // End of an author name, add it to results if not empty
          val authorName = currentAuthor.toString().trim
          if (!authorName.isEmpty) {
            result += authorName
          }
          currentAuthor = new StringBuilder()

        case _ =>
          // Add character to current author name
          currentAuthor.append(c)
      }
    }

    // Add the last author if any
    val lastAuthor = currentAuthor.toString().trim
    if (!lastAuthor.isEmpty) {
      result += lastAuthor
    }

    result.toList
  }

  // Parse CSV line respecting quotes - exact implementation from booksDataExtractSortAuthors.scala
  def parseCSVLine(line: String): Array[String] = {
    val result = new scala.collection.mutable.ArrayBuffer[String]
    var current = new StringBuilder
    var inQuotes = false

    for (c <- line) {
      c match {
        case '"' => inQuotes = !inQuotes; current.append(c)
        case ',' if !inQuotes =>
          result += current.toString.trim
          current = new StringBuilder
        case _ => current.append(c)
      }
    }

    if (current.nonEmpty) {
      result += current.toString.trim
    }

    result.toArray
  }
}