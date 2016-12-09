package com.cxl.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chenxiaolei on 16/12/9.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("wordcound")
    val sc = new SparkContext(conf)
    val line = sc.textFile("file:////Users/chenxiaolei/word.txt")
    val word = line.flatMap(_.split("\t"))
    val tuple = word.mapPartitions(x => {
      x.map(w => {
        (w,1)
      })
    })
    tuple.reduceByKey(_+_).sortBy(_._2,false).foreachPartition(x => x.foreach(println(_)))







  }
}
