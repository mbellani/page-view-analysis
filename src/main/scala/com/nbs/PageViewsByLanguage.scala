package com.nbs

import java.net.URLDecoder

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

case class PageViews(language: String, page: String, count: Long) extends Serializable {
  //url decoder is here -- should this be at the source?
  override def toString: String = s"$language | ${URLDecoder.decode(page, "UTF-8")} | $count"
}

class PageViewsByLanguage(df: DataFrame) extends Serializable {

  def aggr(): Seq[PageViews] = {
    aggregatePageViews(df).registerTempTable("aggr_page_views")
    top10ForEachLanguage(df)
  }

  def top10ForEachLanguage(df: DataFrame): Seq[PageViews] = {
    allLanguages(df.sqlContext).flatMap(l => df.sqlContext.sql("select language, page, views from aggr_page_views")
      .where(df("language").equalTo(l)).limit(10)
      .map(toPageView(_))
      .collect())
  }

  private def toPageView(r: Row): PageViews = {
    new PageViews(language = r.getString(0),
      page = r.getString(1),
      count = r.getLong(2))
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