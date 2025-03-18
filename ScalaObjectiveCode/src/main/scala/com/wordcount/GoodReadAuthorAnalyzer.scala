package com.wordcount


import org.apache.spark.{SparkConf, SparkContext}
import java.io.{File, PrintWriter}

object GoodReadAuthorAnalyzer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("AuthorMatchAnalyzer")
      .setMaster("local[*]")
      .set("spark.driver.memory", "4g")
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.bindAddress", "127.0.0.1")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    try {
      val startTime = System.currentTimeMillis()

      // Paths to input files
      val goodAuthorsPath = "C:/Users/akuca/Desktop/bdProject/good_authors.txt"
      val badAuthorsPath = "C:/Users/akuca/Desktop/bdProject/bad_authors.txt"
      val neutralAuthorsPath = "C:/Users/akuca/Desktop/bdProject/neutral_authors.txt"
      val goodreadsAuthorsPath = "C:/Users/akuca/Desktop/bdProject/authors_data_clean.csv"

      // Output paths for matched authors
      val matchedGoodAuthorsPath = "C:/Users/akuca/Desktop/bdProject/matched_good_authors.txt"
      val matchedBadAuthorsPath = "C:/Users/akuca/Desktop/bdProject/matched_bad_authors.txt"
      val matchedNeutralAuthorsPath = "C:/Users/akuca/Desktop/bdProject/matched_neutral_authors.txt"
      val summaryPath = "C:/Users/akuca/Desktop/bdProject/author_matches_summary.txt"

      //Read classified author lists
      println("Reading classified author lists...")
      val goodAuthors = sc.textFile(goodAuthorsPath).map(_.trim).filter(_.nonEmpty).cache()
      val badAuthors = sc.textFile(badAuthorsPath).map(_.trim).filter(_.nonEmpty).cache()
      val neutralAuthors = sc.textFile(neutralAuthorsPath).map(_.trim).filter(_.nonEmpty).cache()

      //Extract authors from Goodreads dataset - using the same logic as authorsDataExtractSortAuthors.scala
      println("Extracting authors from Goodreads dataset...")
      val goodreadsAuthorsRDD = sc.textFile(goodreadsAuthorsPath)
      val goodreadsHeader = goodreadsAuthorsRDD.first()

      // Find author name column index from header (as in authorsDataExtractSortAuthors.scala)
      val headers = goodreadsHeader.split(",").map(_.trim.replaceAll("^\"|\"$", ""))
      val nameIndex = headers.indexWhere(_.equals("name"))

      if (nameIndex == -1) {
        println("Error: No 'name' column found in the Goodreads authors CSV file.")
        return
      }

      // Extract author names using the same parseCSVLine logic as in authorsDataExtractSortAuthors.scala
      val goodreadsAuthors = goodreadsAuthorsRDD
        .filter(_ != goodreadsHeader)
        .map(line => {
          val fields = parseCSVLine(line)
          if (fields.length > nameIndex) {
            fields(nameIndex).trim.replaceAll("^\"|\"$", "")
          } else {
            ""
          }
        })
        .filter(_.nonEmpty)
        .distinct()
        .cache()

      // Find matches for each author category
      println("Finding matches for each author category...")

      // Find matches using Spark's intersection
      val matchedGoodAuthors = goodAuthors.intersection(goodreadsAuthors).cache()
      val matchedBadAuthors = badAuthors.intersection(goodreadsAuthors).cache()
      val matchedNeutralAuthors = neutralAuthors.intersection(goodreadsAuthors).cache()

      // Collect statistics
      val totalGoodAuthors = goodAuthors.count()
      val totalBadAuthors = badAuthors.count()
      val totalNeutralAuthors = neutralAuthors.count()
      val totalGoodreadsAuthors = goodreadsAuthors.count()

      val matchedGoodCount = matchedGoodAuthors.count()
      val matchedBadCount = matchedBadAuthors.count()
      val matchedNeutralCount = matchedNeutralAuthors.count()
      val totalMatchedCount = matchedGoodCount + matchedBadCount + matchedNeutralCount

      // Write matches to output files
      println("Writing matches to output files...")

      // Write matched good authors
      writeRDDToFile(matchedGoodAuthors, matchedGoodAuthorsPath)

      // Write matched bad authors
      writeRDDToFile(matchedBadAuthors, matchedBadAuthorsPath)

      // Write matched neutral authors
      writeRDDToFile(matchedNeutralAuthors, matchedNeutralAuthorsPath)

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

    } finally {
      sc.stop()
    }
  }

  // Helper method to write an RDD to a file
  def writeRDDToFile(rdd: org.apache.spark.rdd.RDD[String], outputPath: String): Unit = {
    val data = rdd.collect()

    // Write to file
    val writer = new PrintWriter(new File(outputPath))
    try {
      data.foreach(line => writer.println(line))
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

  // Parse CSV line respecting quotes - same implementation from authorsDataExtractSortAuthors.scala
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