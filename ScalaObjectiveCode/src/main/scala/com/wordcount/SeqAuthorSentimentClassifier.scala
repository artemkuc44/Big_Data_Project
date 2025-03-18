package com.wordcount

import java.io.{BufferedReader, File, FileReader, PrintWriter}
import scala.collection.mutable.{HashMap, HashSet, ListBuffer}

object SeqAuthorSentimentClassifier {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()

    // Path to CSV files
    val booksDataFilePath = "C:/Users/akuca/Desktop/bdProject/books_data_clean.csv"
    val reviewsFilePath = "C:/Users/akuca/Desktop/bdProject/books_rating_clean.csv"

    // Add "seq" to output file names to differentiate from Spark version
    val goodAuthorsOutputPath = "C:/Users/akuca/Desktop/bdProject/seq_good_authors.txt"
    val badAuthorsOutputPath = "C:/Users/akuca/Desktop/bdProject/seq_bad_authors.txt"
    val neutralAuthorsOutputPath = "C:/Users/akuca/Desktop/bdProject/seq_neutral_authors.txt"

    // Separate limits for books and reviews to reduce runtime
    val booksProcessLimit = 212400
    val reviewsProcessLimit = 3000000

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

    // STEP 1: Extract authors and create title-to-authors mapping
    println("Reading books data to extract all authors...")

    // Read books data file
    val booksReader = new BufferedReader(new FileReader(booksDataFilePath))
    val booksHeader = booksReader.readLine()

    // Get headers to find column indexes
    val headers = parseCSVLine(booksHeader).map(_.trim.replaceAll("^\"|\"$", ""))
    val authorsIndex = headers.indexWhere(_.equals("authors"))
    val titleIndex = headers.indexWhere(_.equals("Title"))

    if (authorsIndex == -1 || titleIndex == -1) {
      println("Error: Required columns not found in books CSV file.")
      return
    }

    // Process books data
    val allAuthorsSet = new HashSet[String]()
    val titleToAuthorsMap = new HashMap[String, List[String]]()

    var booksLine: String = null
    var booksCounter = 0

    while ({booksLine = booksReader.readLine(); booksLine != null && booksCounter < booksProcessLimit}) {
      val fields = parseCSVLine(booksLine)

      if (fields.length > Math.max(authorsIndex, titleIndex)) {
        val title = fields(titleIndex).trim
        val authorsField = fields(authorsIndex).trim

        if (!authorsField.isEmpty && authorsField != "[]") {
          val start = authorsField.indexOf('[')
          val end = authorsField.lastIndexOf(']')

          if (start >= 0 && end > start) {
            val bracketContent = authorsField.substring(start + 1, end).trim
            val authors = extractAuthorsFromBracketContent(bracketContent)

            // Add authors to the set of all authors
            authors.foreach(allAuthorsSet.add)

            // Add title-to-authors mapping if title is not empty
            if (!title.isEmpty) {
              titleToAuthorsMap.put(title, authors)
            }
          }
        }
      }

      booksCounter += 1
    }

    booksReader.close()
    println(s"Processed $booksCounter book records (limit: $booksProcessLimit)")

    // STEP 2: Process reviews to calculate title sentiment scores
    println("Reading reviews to calculate title sentiment scores...")

    // Read reviews file
    val reviewsReader = new BufferedReader(new FileReader(reviewsFilePath))
    val reviewsHeader = reviewsReader.readLine()

    // Find review text column indexes
    val reviewsHeaders = parseCSVLine(reviewsHeader).map(_.trim.replaceAll("^\"|\"$", ""))
    val reviewTitleIndex = reviewsHeaders.indexWhere(_.equals("Title"))
    val reviewTextIndex = reviewsHeaders.indexWhere(_.equals("review/text"))

    if (reviewTitleIndex == -1 || reviewTextIndex == -1) {
      println("Error: Required columns not found in reviews file.")
      return
    }

    // Process reviews
    val titleSentimentMap = new HashMap[(String, String), Int]()  // Map of (title, sentiment_type) -> count

    var reviewsLine: String = null
    var reviewsCounter = 0

    while ({reviewsLine = reviewsReader.readLine(); reviewsLine != null && reviewsCounter < reviewsProcessLimit}) {
      val fields = parseCSVLine(reviewsLine)

      if (fields.length > Math.max(reviewTitleIndex, reviewTextIndex)) {
        val title = fields(reviewTitleIndex).trim
        val reviewText = fields(reviewTextIndex).trim

        if (!title.isEmpty && !reviewText.isEmpty) {
          // Split review into words
          val words = reviewText.toLowerCase.split("\\W+")

          // Count positive and negative words
          val positiveCount = words.count(positiveWords.contains)
          val negativeCount = words.count(negativeWords.contains)

          // Update sentiment counts
          val posKey = (title, "positive")
          val negKey = (title, "negative")

          titleSentimentMap.put(posKey, titleSentimentMap.getOrElse(posKey, 0) + positiveCount)
          titleSentimentMap.put(negKey, titleSentimentMap.getOrElse(negKey, 0) + negativeCount)
        }
      }

      reviewsCounter += 1
    }

    reviewsReader.close()
    println(s"Processed $reviewsCounter review records (limit: $reviewsProcessLimit)")

    // Calculate title sentiment scores
    val titleScores = new HashMap[String, Double]()

    // Group by title and calculate scores
    val titleSet = titleSentimentMap.keySet.map(_._1).toSet

    for (title <- titleSet) {
      val posCount = titleSentimentMap.getOrElse((title, "positive"), 0)
      val negCount = titleSentimentMap.getOrElse((title, "negative"), 0)
      val totalWords = posCount + negCount

      if (totalWords > 0) {
        val sentimentScore = (posCount - negCount).toDouble / totalWords.toDouble
        titleScores.put(title, sentimentScore)
      }
    }

    // Calculate author sentiment scores
    println("Calculating author sentiment scores...")

    val authorScores = new HashMap[String, (Double, Int)]()  // Map of author -> (scoreSum, count)

    for ((title, authors) <- titleToAuthorsMap if titleScores.contains(title)) {
      val score = titleScores(title)
      for (author <- authors) {
        val current = authorScores.getOrElse(author, (0.0, 0))
        authorScores.put(author, (current._1 + score, current._2 + 1))
      }
    }

    // Calculate average scores
    val authorAvgScores = new HashMap[String, Double]()

    for ((author, (scoreSum, count)) <- authorScores) {
      val avgScore = if (count > 0) scoreSum / count else 0.0
      authorAvgScores.put(author, avgScore)
    }

    // STEP 4: Classify all authors
    println("Classifying all authors...")

    val goodAuthors = new ListBuffer[String]()
    val badAuthors = new ListBuffer[String]()
    val neutralAuthors = new ListBuffer[String]()

    for (author <- allAuthorsSet) {
      val score = authorAvgScores.getOrElse(author, 0.0)

      if (score > 0.0) {
        goodAuthors += author
      } else if (score < 0.0) {
        badAuthors += author
      } else {
        neutralAuthors += author
      }
    }

    // Write results to files
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
    println("\nAuthor Sentiment Classification Results (Sequential):")
    println("------------------------------------------")
    println(s"Total unique authors: ${allAuthorsSet.size}")
    println(s"Authors with sentiment data: ${authorScores.size}")
    println(s"Authors classified as good (score > 0.0): ${goodAuthors.size}")
    println(s"Authors classified as bad (score < 0.0): ${badAuthors.size}")
    println(s"Authors classified as neutral (score = 0.0 or no reviews): ${neutralAuthors.size}")
    println(s"Results saved to: $goodAuthorsOutputPath, $badAuthorsOutputPath, and $neutralAuthorsOutputPath")
    println(f"Execution time: $executionTime%.2f seconds")
    println(s"Books processed: $booksCounter (limit: $booksProcessLimit)")
    println(s"Reviews processed: $reviewsCounter (limit: $reviewsProcessLimit)")
  }

  // Extract authors from bracket content - keeping same logic as Spark version
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

  // Parse CSV line respecting quotes - same as Spark version
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