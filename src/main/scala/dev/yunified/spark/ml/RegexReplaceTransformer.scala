package dev.yunified.spark.ml

import scala.util.matching.Regex
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class RegexReplaceTransformer(override val uid: String) extends Transformer {
 type RegexReplacements = List[(Regex, String)]

 final val inputCol = new Param[String](this, "inputCol", "The input column")
 final val outputCol = new Param[String](this, "outputCol", "The output column")
 final val regexReplacePairs = new Param[RegexReplacements](this, "regexReplacements",
   "A list of regex-replacements tuples")

 def setInputCol(value: String): this.type = set(inputCol, value)
 def setOutputCol(value: String): this.type = set(outputCol, value)
 def setRegexReplacePairs(value: RegexReplacements): this.type = set(regexReplacePairs, value)

 def this() = this(Identifiable.randomUID("RegexReplaceTransformer"))

 def copy(extra: ParamMap): RegexReplaceTransformer = {
   defaultCopy(extra)
 }

 override def transformSchema(schema: StructType): StructType = {
   // check that the input type is a string (can't regex non-strings)
   val index = schema.fieldIndex($(inputCol))
   val field = schema.fields(index)
   if (field.dataType != StringType) {
     throw new Exception(s"Input type ${field.dataType} needs to be a StringType")
   }
   // Add new field and return the schema
   schema.add(StructField($(outputCol), StringType, nullable = true))
 }

 private def regexReplaceList(regexPairs: RegexReplacements) =
   udf[String, String](
     input =>
     regexPairs.foldLeft(input)(( curString, regReplacePair) => {
       val (regexpression, replacement) = regReplacePair
       regexpression.replaceAllIn(curString, replacement)
     })
   )

 def transform(df: Dataset[_]): DataFrame = {
   df.withColumn(
     $(outputCol),
     regexReplaceList($(regexReplacePairs))(col($(inputCol)))
   )
 }
}
