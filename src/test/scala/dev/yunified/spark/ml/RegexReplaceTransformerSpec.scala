package dev.yunified.spark.ml

import scala.util.matching.Regex

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import com.github.mrpowers.spark.fast.tests.DatasetComparer

class RegexReplaceTransformerSpec 
  extends AnyFlatSpec 
  with LocalSparkSessionTestWrapper 
  with DatasetComparer {

 import spark.implicits._

 "RegexReplaceTransformer" should "makes multipe regex replacements on the same string" in {
   val inputDF: DataFrame = Seq(
     (1, "12 test strings for dog"),
     (2, "Not dog more 1 donut")
   ).toDF("id", "text")

   val outputDF: DataFrame = Seq(
     (1, "[number] test strings for [cat]"),
     (2, "Not [cat] more [number] donut")
   ).toDF("id", "normalizedText")

   val pairs: List[(Regex, String)] = List(
     """dog""".r -> "[cat]",
     """\d+""".r -> "[number]"
   )

   val regexReplace = new RegexReplaceTransformer()
     .setInputCol("text")
     .setOutputCol("normalizedText")
     .setRegexReplacePairs(pairs)

   val processedDF = regexReplace.transform(inputDF)

   assertSmallDatasetEquality(
     processedDF.select("id", "normalizedText"),
     outputDF
     )
 }
}
