package com.cxl.pagerank

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chenxiaolei on 16/12/12.
  */
object PageRank {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("pagerank")
    val sc = new SparkContext(conf)
    val links = sc.parallelize(Array(("a",Array("d")),("b",Array("a")),("c",Array("a","b")),("d",Array("a","c")))).map(x =>(x._1,x._2)).cache()
    var rank = sc.parallelize(Array(("a",1.0),("b",1.0),("c",1.0),("d",1.0)))

    for (i <- 1 to 2 ){
    val con  = links.join(rank).flatMap{
      case (url,(link,rank)) => {
        link.map(x => (x,rank/link.size))
      }
    }
    rank = con.reduceByKey(_+_).mapValues(0.15+0.85*_)
    }


    rank.foreach(println(_))



  }

}
