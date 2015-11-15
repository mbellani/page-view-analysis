package com.nbs

import java.net.URLDecoder

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame, SQLContext}

case class PageViews(language: String, page: String, count: Long) {
  //url decoder is here -- should this be at the source?
  override def toString: String = s"$language | ${URLDecoder.decode(page, "UTF-8")} | $count"
}

class PageViewsByLanguage(sc: SparkContext) extends Serializable {
  val sqlContext = new SQLContext(sc)

  def aggr(): Seq[PageViews] = {
    val df = load(sqlContext)
    aggregatePageViews(df).registerTempTable("aggr_page_views")
    top10ForEachLanguage(sqlContext, df)
  }

  def top10ForEachLanguage(sqLContext: SQLContext, df: DataFrame): Seq[PageViews] = {
    allLanguages(sqlContext).flatMap(l => sqlContext.sql("select language, page, views from aggr_page_views")
      .where(df("language").equalTo(l)).limit(10)
      .map(toPageView(_))
      .collect())
  }

  private def toPageView(r: Row): PageViews = {
    new PageViews(language = r.getString(0),
      page = r.getString(1),
      count = r.getLong(2))
  }

  private def schema(): StructType = {
    StructType(List(
      new StructField("language", StringType),
      new StructField("page", StringType),
      new StructField("views", IntegerType),
      new StructField("bytes_transferred", LongType)))
  }


  private def load(sqlContext: SQLContext): DataFrame = {
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", "|")
      .schema(schema)
      .load("hdfs://localhost:9000/wikipedia/page-views.txt")
    df
  }

  private def aggregatePageViews(df: DataFrame): DataFrame = {
    df.filter(!df("language").contains(".") && !df("page").contains(":"))
      .groupBy("language", "page")
      .agg(sum("views").as("views"))
      .sort(desc("views"))
      .cache()
  }

  def allLanguages(sqlContext: SQLContext): Array[String] = {
    val languages = sqlContext.sql("select distinct(language) from aggr_page_views order by language")
      .map(r => r.getString(0))
      .collect()
    languages
  }
}