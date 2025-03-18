package com.wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkWordCount {
  def main(args: Array[String]): Unit = {
    // Configure Spark
    val conf = new SparkConf()
      .setAppName("CSVReviewWordCount")
      .setMaster("local[*]")
      .set("spark.driver.memory", "4g")
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.bindAddress", "127.0.0.1")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    try {
      val startTime = System.currentTimeMillis()

      // Path to CSV file
      val filePath = "C:/Users/akuca/Desktop/bdProject/books_rating_clean.csv"

      // Read CSV file
      val csvLines = sc.textFile(filePath)

      // Get header to identify column positions
      val header = csvLines.first()

      // Skip header and parse CSV rows
      val reviewTexts = csvLines
        .filter(_ != header)
        .map(line => {
          // Split by comma, but respect quotes (handle commas within quotes)
          val fields = parseCSVLine(line)
          if (fields.length >= 10) fields(9) else ""
        })
        .filter(_.nonEmpty)

      // Process words
      val words = reviewTexts.flatMap(review => review.toLowerCase.split("\\W+"))

      val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
      val topWords = wordCounts.sortBy(-_._2).take(10)

      val executionTime = (System.currentTimeMillis() - startTime) / 1000.0

      // Print results
      println(s"\nTop 10 words in reviews:")
      topWords.foreach(pair => println(f"${pair._1}: ${pair._2}"))
      println(f"Execution time: $executionTime%.2f seconds")

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