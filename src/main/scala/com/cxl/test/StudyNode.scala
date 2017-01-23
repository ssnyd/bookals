package com.cxl.test

/**
  * Created by chenxiaolei on 16/12/12.
  */
object StudyNode {
  def main(args: Array[String]): Unit = {

    val map: Map[String, String] = Map("chen" -> "lei")
    val r1: Option[String] = map.get("chen")
    val r2: Option[String] = map.get("chen1")
    println(r1)
    println(r2)

    //    查看关系表
    //    val rawUserArtistData = sc.textFile("file://///Users/chenxiaolei/Downloads/profiledata_06-May-2005/user_artist_data.txt")
    //    rawUserArtistData.filter(x => {
    //      x.split(" ")(2).toInt > 10000
    //    }).foreachPartition(x =>
    //    x.foreach(println(_))
    //    )
  }

}
