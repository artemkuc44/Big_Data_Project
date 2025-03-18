package com.wordcount


import scala.io.Source
import java.io.{File, PrintWriter}

object authorsDataExtractSortAuthors {
  def main(args: Array[String]): Unit = {
    val filePath = "C:/Users/akuca/Desktop/bdProject/authors_data_clean.csv"
    val outputPath = "C:/Users/akuca/Desktop/bdProject/authors_data_authors_list.txt"

    try {
      val source = Source.fromFile(filePath)
      val lines = source.getLines().toList
      source.close()

      // Find name column index
      val headers = lines.head.split(",").map(_.trim.replaceAll("^\"|\"$", ""))
      val nameIndex = headers.indexWhere(_.equals("name"))

      if (nameIndex == -1) {
        println("Error: No 'name' column found in the CSV file.")
        return
      }

      // List to collect all author names
      val authorNames = scala.collection.mutable.ListBuffer[String]()

      // Process each data row
      for (line <- lines.tail) {
        val fields = parseCSVLine(line)

        if (fields.length > nameIndex) {
          val name = fields(nameIndex).trim.replaceAll("^\"|\"$", "")
          if (name.nonEmpty) {
            authorNames += name
          }
        }
      }

      // Sort the authors alphabetically and remove duplicates
      val sortedAuthors = authorNames.distinct.sorted

      // Write sorted authors to a text file
      val writer = new PrintWriter(new File(outputPath))
      writer.write(sortedAuthors.mkString(", "))
      writer.close()

      println(s"Successfully wrote ${sortedAuthors.size} sorted authors to $outputPath")

      // Print first 10 authors as a preview
      println("\nFirst 10 authors (preview, sorted alphabetically):")
      sortedAuthors.take(10).zipWithIndex.foreach { case (author, index) =>
        println(s"${index + 1}: $author")
      }

    } catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  // Simple CSV line parser that respects quotes
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
