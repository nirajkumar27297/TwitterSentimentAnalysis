package com.bridgelabz.TwitterSentimentAnalysisPackage
import java.text.Normalizer

import scala.collection.mutable

object CleansingOperations {
  def extractingTextPart(review: String): String = {

    val reviews = Normalizer.normalize(review, Normalizer.Form.NFD)
    reviews.toLowerCase
      .replaceAll("[^\\p{ASCII}]", " ")
      .replaceAll("@\\w+ *", " ") //removing words starting with @
      .replaceAll(
        "https?://\\S+",
        " "
      ) //removing words starting with http ot https
      .replaceAll(
        "[^a-z]",
        " "
      )
      .replaceAll(
        """\b\w{1,2}\b""",
        " "
      ) // removing words starting which are not alphabets
      .toLowerCase
      .replaceAll("\\s{2,}", " ")
      .trim

  }

  def tokenizer(review: String): Array[String] = {
    review.split(" ")
  }
  def combineWords(review: mutable.WrappedArray[String]): String = {
    review.mkString(" ")
  }

}
