package com.wordcount

import scala.io.Source
import java.io.File

object CompareAuthorLists {
  def main(args: Array[String]): Unit = {
    // File paths for both author lists
    val file1Path = "C:/Users/akuca/Desktop/bdProject/books_data_authors_list.txt"
    val file2Path = "C:/Users/akuca/Desktop/bdProject/authors_data_authors_list.txt"

    try {
      // Read both files
      val authorsFile1 = readAuthorsFromFile(file1Path)
      val authorsFile2 = readAuthorsFromFile(file2Path)

      println(s"Read ${authorsFile1.size} authors from books_data_authors_list.txt")
      println(s"Read ${authorsFile2.size} authors from authors_list_sorted.txt")

      // Find matches
      val matches = findMatches(authorsFile1, authorsFile2)

      // Print results
      println(s"\nFound ${matches.size} matching author names in both files.")

      // Print first 10 matches as a sample
      if (matches.nonEmpty) {
        println("\nSample of matching names (first 100):")
        matches.take(100).zipWithIndex.foreach { case (name, index) =>
          println(s"${index + 1}. $name")
        }
      }

    } catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  // Function to read comma-separated authors from a file
  def readAuthorsFromFile(filePath: String): Set[String] = {
    val source = Source.fromFile(filePath)
    val content = source.mkString
    source.close()

    // Split by comma and trim each name
    content.split(",").map(_.trim).filter(_.nonEmpty).toSet
  }

  // Function to find matching names between two sets
  def findMatches(set1: Set[String], set2: Set[String]): Set[String] = {
    // Use Set intersection to find matches efficiently
    val matches = set1.intersect(set2)
    matches
  }
}