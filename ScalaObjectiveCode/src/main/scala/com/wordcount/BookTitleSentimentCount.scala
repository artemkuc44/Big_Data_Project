package com.wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object BookTitleSentimentCount {
  def main(args: Array[String]): Unit = {
    // Configure Spark
    val conf = new SparkConf()
      .setAppName("BookTitleSentimentCount")
      .setMaster("local[*]")
      .set("spark.driver.memory", "4g")
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.bindAddress", "127.0.0.1")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    try {
      val startTime = System.currentTimeMillis()

      val reviewsFilePath = "C:/Users/akuca/Desktop/bdProject/books_rating_clean.csv"

      val negativeWords = Set(
        "bad", "poor", "terrible", "awful", "horrible",
        "negative", "worse", "worst", "disappointing", "disappointment",
        "mediocre", "failure", "failed", "useless", "boring",
        "waste", "dislike", "hate", "dreadful", "lousy"
      )

      val positiveWords = Set(
        "good", "great", "excellent", "amazing", "wonderful",
        "positive", "better", "best", "outstanding", "fantastic",
        "brilliant", "enjoyable", "favorite", "loved", "love",
        "impressive", "perfect", "superb", "delightful", "beautiful"
      )

      val reviewsLines = sc.textFile(reviewsFilePath)
      val reviewsHeader = reviewsLines.first()

      // Parse and extract (title, review_text) pairs
      val titleReviewPairs = reviewsLines
        .filter(_ != reviewsHeader)
        .map(line => {
          val fields = parseCSVLine(line)
          if (fields.length >= 10) (fields(1), fields(9)) else ("", "")
        })
        .filter(_._1.nonEmpty)

      // For each title, analyze sentiment in review text
      val titleSentiment = titleReviewPairs.flatMap { case (title, reviewText) =>
        // Split review into words
        val words = reviewText.toLowerCase.split("\\W+")

        // Count positive words
        val positiveCount = words.count(positiveWords.contains)

        // Count negative words
        val negativeCount = words.count(negativeWords.contains)

        // Emit pairs of (title, sentiment_type) -> count
        Seq(
          ((title, "positive"), positiveCount),
          ((title, "negative"), negativeCount)
        )
      }

      // Aggregate counts by title and sentiment type
      val aggregatedCounts = titleSentiment
        .reduceByKey(_ + _)
        .map { case ((title, sentimentType), count) => (title, (sentimentType, count)) }
        .groupByKey()
        .mapValues { sentiments =>
          val sentimentMap = sentiments.toMap
          (sentimentMap.getOrElse("positive", 0), sentimentMap.getOrElse("negative", 0))
        }
        // Filter out titles with no sentiment words
        .filter { case (_, (posCount, negCount)) => posCount + negCount > 0 }

      // Calculate normalized sentiment score and create tuples with all info
      val titlesWithScore = aggregatedCounts.map { case (title, (posCount, negCount)) =>
        // Calculate normalized sentiment score: (positive - negative) / total
        val totalWords = posCount + negCount
        val sentimentScore = (posCount - negCount).toDouble / totalWords.toDouble

        (title, posCount, negCount, totalWords, sentimentScore)
      }

      // Get top 10 titles with best reviews (highest sentiment score)
      val top10BestTitles = titlesWithScore
        .filter { case (_, _, _, total, _) =>
          // Ensure we have meaningful data (at least x sentiment words total)
          total >= 50
        }
        .sortBy(-_._5)  // Sort by sentiment score in descending order
        .take(10)

      // Get top 10 titles with worst reviews (lowest sentiment score)
      val top10WorstTitles = titlesWithScore
        .filter { case (_, _, _, total, _) =>
          // Ensure we have meaningful data (at least x sentiment words total)
          total >= 50
        }
        .sortBy(_._5)  // Sort by sentiment score in ascending order
        .take(10)

      val executionTime = (System.currentTimeMillis() - startTime) / 1000.0

      // Print top 10 best reviewed titles
      println("\nTop 10 Book Titles with Best Reviews (Highest Sentiment Score):")
      println("-------------------------------------------------------------------")
      println("%-60s | %-12s | %-12s | %-12s | %-15s".format(
        "Title", "Positive", "Negative", "Total Words", "Sentiment Score"))
      println("-" * 118)

      top10BestTitles.foreach { case (title, posCount, negCount, total, score) =>
        val truncatedTitle = if (title.length > 57) title.substring(0, 57) + "..." else title
        println("%-60s | %-12d | %-12d | %-12d | %+.2f".format(
          truncatedTitle, posCount, negCount, total, score))
      }

      println("\nTop 10 Book Titles with Worst Reviews (Lowest Sentiment Score):")
      println("-------------------------------------------------------------------")
      println("%-60s | %-12s | %-12s | %-12s | %-15s".format(
        "Title", "Positive", "Negative", "Total Words", "Sentiment Score"))
      println("-" * 118)

      top10WorstTitles.foreach { case (title, posCount, negCount, total, score) =>
        val truncatedTitle = if (title.length > 57) title.substring(0, 57) + "..." else title
        println("%-60s | %-12d | %-12d | %-12d | %+.2f".format(
          truncatedTitle, posCount, negCount, total, score))
      }

      println(f"\nExecution time: $executionTime%.2f seconds")

    } finally {
      sc.stop()
    }
  }

  def parseCSVLine(line: String): Array[String] = {
    val result = new scala.collection.mutable.ArrayBuffer[String]()
    var current = new StringBuilder()
    var inQuotes = false

    for (c <- line) {
      if (c == '"') {
        inQuotes = !inQuotes
      } else if (c == ',' && !inQuotes) {
        result += current.toString()
        current = new StringBuilder()
      } else {
        current += c
      }
    }

    // Add the last field
    result += current.toString()

    // Remove surrounding quotes from fields
    result.map(field =>
      if (field.startsWith("\"") && field.endsWith("\""))
        field.substring(1, field.length - 1)
      else field
    ).toArray
  }
}