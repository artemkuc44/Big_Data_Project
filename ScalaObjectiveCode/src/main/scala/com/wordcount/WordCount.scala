package com.wordcount


import scala.io.Source
import scala.collection.mutable.{Map => MutableMap}
import java.io.File

object WordCount{
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val wordCount = MutableMap[String, Int]()

    // Set the number of rows to process
    val rowLimit = 1000

    val filePath = "C:/Users/akuca/Desktop/bdProject/books_rating_clean.csv"

    try {
      // Read and process the CSV file
      val source = Source.fromFile(filePath, "UTF-8")

      try {
        val lines = source.getLines().buffered

        // Process header line to find review/text column index
        val headerLine = if (lines.hasNext) lines.next() else ""
        val headers = parseCsvLine(headerLine)
        val reviewTextIndex = headers.indexWhere(_.trim == "review/text")

        if (reviewTextIndex >= 0) {
          var rowsProcessed = 0

          // Process rows up to the limit
          while (lines.hasNext && rowsProcessed < rowLimit) {
            val line = lines.next()
            val columns = parseCsvLine(line)

            // Get the review text if the index is valid
            if (columns.length > reviewTextIndex) {
              val reviewText = columns(reviewTextIndex)

              if (reviewText.nonEmpty) {
                // Extract words using regex (equivalent to re.findall in Python)
                val words = "\\b\\w+\\b".r.findAllIn(reviewText.toLowerCase).toList

                // Update word counts
                words.foreach { word =>
                  wordCount.put(word, wordCount.getOrElse(word, 0) + 1)
                }
              }
            }

            rowsProcessed += 1
          }
        } else {
          println("Column 'review/text' not found in CSV header")
        }
      } finally {
        source.close()
      }

      // Get the top 10 most common words
      val top10Words = wordCount.toSeq.sortBy(-_._2).take(10)

      val endTime = System.currentTimeMillis()

      println(s"Top 10 Most Used Words (from partial dataset with $rowLimit entries):")
      top10Words.foreach { case (word, count) =>
        println(s"$word: $count")
      }

      println(s"Execution time (partial dataset): ${(endTime - startTime) / 1000.0} seconds")
    } catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  // Helper function to parse CSV lines (handles quotes properly)
  def parseCsvLine(line: String): Array[String] = {
    // Split by comma, but respect quotes
    line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
      .map(field => field.replaceAll("^\"|\"$", "").trim()) // Remove surrounding quotes and trim
  }
}