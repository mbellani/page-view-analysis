package com.nbs

import org.apache.spark.{SparkConf, SparkContext}

object PageViewAnalysis {

  def main(args: Array[String]) {
    val sc = new SparkContext(conf)
    val views = new PageViewsByLanguage(sc).aggr()
    display(views)
  }

  def display(views: Seq[PageViews]): Unit = {
    views.foreach(println(_))
    println("-------------------")
  }

  def conf: SparkConf = {
    new SparkConf().setAppName("page-view-analysis")
  }
}
