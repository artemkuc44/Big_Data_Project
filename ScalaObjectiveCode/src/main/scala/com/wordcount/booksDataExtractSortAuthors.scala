package com.wordcount

import scala.io.Source
import java.io.{File, PrintWriter}

object booksDataExtractSortAuthors {
  def main(args: Array[String]): Unit = {
    val filePath = "C:/Users/akuca/Desktop/bdProject/books_data_clean.csv"
    val outputPath = "C:/Users/akuca/Desktop/bdProject/books_data_authors_list.txt"

    try {
      // Read CSV file
      val source = Source.fromFile(filePath)
      val lines = source.getLines().toList
      source.close()

      // Find authors column index
      val headers = lines.head.split(",").map(_.trim.replaceAll("^\"|\"$", ""))
      val authorsIndex = headers.indexWhere(_.equals("authors"))

      if (authorsIndex == -1) {
        println("Error: No 'authors' column found in the CSV file.")
        return
      }

      // List to collect all authors
      val allAuthors = scala.collection.mutable.ListBuffer[String]()

      // Process each data row
      for (line <- lines.tail) {
        // Get the raw field content (handling commas within quotes properly)
        val fields = parseCSVLine(line)

        if (fields.length > authorsIndex) {
          val authorsField = fields(authorsIndex).trim

          // Skip if empty or just []
          if (authorsField.isEmpty || authorsField == "[]") {
            // Skip empty fields
          } else {
            // Extract content within square brackets
            val start = authorsField.indexOf('[')
            val end = authorsField.lastIndexOf(']')

            if (start >= 0 && end > start) {
              val bracketContent = authorsField.substring(start + 1, end).trim

              // Extract authors using custom method that handles apostrophes
              val authors = extractAuthorsFromBracketContent(bracketContent)
              allAuthors ++= authors
            }
          }
        }
      }

      // Sort the authors alphabetically
      val sortedAuthors = allAuthors.distinct.sorted

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

  // Parse CSV line respecting quotes
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

  // Custom method to extract authors from the bracket content
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
}