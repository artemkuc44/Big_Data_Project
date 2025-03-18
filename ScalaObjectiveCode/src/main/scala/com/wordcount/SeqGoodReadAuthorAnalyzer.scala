package com.wordcount

import scala.io.Source
import java.io.{File, PrintWriter}
import scala.collection.mutable.ArrayBuffer

object SeqGoodReadAuthorAnalyzer {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()

    try {
      // Paths to input files
      val goodAuthorsPath = "C:/Users/akuca/Desktop/bdProject/seq_good_authors.txt"
      val badAuthorsPath = "C:/Users/akuca/Desktop/bdProject/seq_bad_authors.txt"
      val neutralAuthorsPath = "C:/Users/akuca/Desktop/bdProject/seq_neutral_authors.txt"
      val goodreadsAuthorsPath = "C:/Users/akuca/Desktop/bdProject/authors_data_clean.csv"

      // Output paths for matched authors
      val matchedGoodAuthorsPath = "C:/Users/akuca/Desktop/bdProject/seq_matched_good_authors.txt"
      val matchedBadAuthorsPath = "C:/Users/akuca/Desktop/bdProject/seq_matched_bad_authors.txt"
      val matchedNeutralAuthorsPath = "C:/Users/akuca/Desktop/bdProject/seq_matched_neutral_authors.txt"
      val summaryPath = "C:/Users/akuca/Desktop/bdProject/seq_author_matches_summary.txt"

      //Read classified author lists
      println("Reading classified author lists...")
      val goodAuthors = readLinesFromFile(goodAuthorsPath)
      val badAuthors = readLinesFromFile(badAuthorsPath)
      val neutralAuthors = readLinesFromFile(neutralAuthorsPath)

      //Extract authors from Goodreads dataset
      println("Extracting authors from Goodreads dataset...")

      val goodreadsAuthors = {
        val source = Source.fromFile(goodreadsAuthorsPath)
        try {
          // Read all lines into memory first
          val allLines = source.getLines().toList
          val goodreadsHeader = allLines.head

          // Find author name column index from header
          val headers = goodreadsHeader.split(",").map(_.trim.replaceAll("^\"|\"$", ""))
          val nameIndex = headers.indexWhere(_.equals("name"))

          if (nameIndex == -1) {
            println("Error: No 'name' column found in the Goodreads authors CSV file.")
            return // This will exit the function
          }

          // Extract author names after fully loading the file
          allLines
            .drop(1) // Skip header
            .map(line => {
              val fields = parseCSVLine(line)
              if (fields.length > nameIndex) {
                fields(nameIndex).trim.replaceAll("^\"|\"$", "")
              } else {
                ""
              }
            })
            .filter(_.nonEmpty)
            .distinct
            .toSet
        } finally {
          source.close()
        }
      }

      // Find matches for each author category
      println("Finding matches for each author category...")

      // Find matches using Set's intersection
      val matchedGoodAuthors = goodAuthors.toSet.intersect(goodreadsAuthors).toSeq.sorted
      val matchedBadAuthors = badAuthors.toSet.intersect(goodreadsAuthors).toSeq.sorted
      val matchedNeutralAuthors = neutralAuthors.toSet.intersect(goodreadsAuthors).toSeq.sorted

      // Collect statistics
      val totalGoodAuthors = goodAuthors.size
      val totalBadAuthors = badAuthors.size
      val totalNeutralAuthors = neutralAuthors.size
      val totalGoodreadsAuthors = goodreadsAuthors.size

      val matchedGoodCount = matchedGoodAuthors.size
      val matchedBadCount = matchedBadAuthors.size
      val matchedNeutralCount = matchedNeutralAuthors.size
      val totalMatchedCount = matchedGoodCount + matchedBadCount + matchedNeutralCount

      // Write matches to output files
      println("Writing matches to output files...")

      // Write matched authors
      writeLinesToFile(matchedGoodAuthors, matchedGoodAuthorsPath)
      writeLinesToFile(matchedBadAuthors, matchedBadAuthorsPath)
      writeLinesToFile(matchedNeutralAuthors, matchedNeutralAuthorsPath)

      // Generate and write summary
      val executionTime = (System.currentTimeMillis() - startTime) / 1000.0
      val summary = generateSummary(
        totalGoodAuthors, totalBadAuthors, totalNeutralAuthors,
        totalGoodreadsAuthors, matchedGoodCount, matchedBadCount,
        matchedNeutralCount, totalMatchedCount, executionTime
      )

      // Write summary to file
      val summaryWriter = new PrintWriter(new File(summaryPath))
      try {
        summaryWriter.println(summary)
      } finally {
        summaryWriter.close()
      }

      // Print summary to console
      println("\n" + summary)

    } catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  // Helper method to read lines from a file
  def readLinesFromFile(path: String): Seq[String] = {
    val source = Source.fromFile(path)
    try {
      // Force immediate evaluation with toList before closing the source
      source.getLines().map(_.trim).filter(_.nonEmpty).toList
    } finally {
      source.close()
    }
  }

  // Helper method to write lines to a file
  def writeLinesToFile(lines: Seq[String], outputPath: String): Unit = {
    val writer = new PrintWriter(new File(outputPath))
    try {
      lines.foreach(line => writer.println(line))
    } finally {
      writer.close()
    }
  }

  def generateSummary(
                       totalGood: Long, totalBad: Long, totalNeutral: Long,
                       totalGoodreads: Long, matchedGood: Long, matchedBad: Long,
                       matchedNeutral: Long, totalMatched: Long, executionTime: Double
                     ): String = {
    val sb = new StringBuilder()

    sb.append("Author Match Analysis Results\n")
    sb.append("==========================\n\n")

    sb.append("Input Statistics:\n")
    sb.append(s"- Total Good Authors: $totalGood\n")
    sb.append(s"- Total Bad Authors: $totalBad\n")
    sb.append(s"- Total Neutral Authors: $totalNeutral\n")
    sb.append(s"- Total Authors in Goodreads Dataset: $totalGoodreads\n\n")

    sb.append("Match Results:\n")
    sb.append(s"- Matched Good Authors: $matchedGood (${percentage(matchedGood, totalGood)}% of good authors)\n")
    sb.append(s"- Matched Bad Authors: $matchedBad (${percentage(matchedBad, totalBad)}% of bad authors)\n")
    sb.append(s"- Matched Neutral Authors: $matchedNeutral (${percentage(matchedNeutral, totalNeutral)}% of neutral authors)\n\n")

    sb.append("Summary:\n")
    sb.append(s"- Total Matched Authors: $totalMatched\n")
    sb.append(s"- Match Rate: ${percentage(totalMatched, totalGood + totalBad + totalNeutral)}% of all classified authors\n")
    sb.append(s"- Execution Time: " + f"${executionTime}%.2f" + " seconds\n")

    sb.toString()
  }

  def percentage(part: Long, total: Long): String = {
    if (total == 0) "0.00"
    else f"${(part.toDouble / total.toDouble) * 100}%.2f"
  }

  // Parse CSV line respecting quotes - same implementation from original code
  def parseCSVLine(line: String): Array[String] = {
    val result = new ArrayBuffer[String]
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