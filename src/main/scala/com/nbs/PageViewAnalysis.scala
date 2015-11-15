package com.nbs

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object PageViewAnalysis {

  def main(args: Array[String]) {
    val sc = new SparkContext()
    val views = new PageViewsByLanguage(loadSQLContext(sc)).aggr()
    display(views)
  }
  private def loadSQLContext(sc: SparkContext): DataFrame = {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", "|")
      .schema(schema)
      .load(sc.getConf.get("spark.data.file.location"))
    df
  }

  def display(views: Seq[PageViews]): Unit = {
    views.foreach(println(_))
    println("-------------------")
  }

  private def schema(): StructType = {
    StructType(List(
      new StructField("language", StringType),
      new StructField("page", StringType),
      new StructField("views", IntegerType),
      new StructField("bytes_transferred", LongType)))
  }

}
