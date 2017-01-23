package com.cxl.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chenxiaolei on 16/12/9.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hello").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val line = sc.textFile("file:////Users/chenxiaolei/stdout")
    line.filter(_.contains("2017-01-19")).foreach(println(_))





//    line.filter(x =>
//      //{road=虹桥广场106号-13-9, sex=女, name=宰莲琦, tel=13703159061, email=umepifwy@yahoo.com.cn}
//    x.contains("}")&& x.contains("{")
//    ).map{
//      x =>
//        val li = x.replace("}","").replace("{","").split(",")
//        li(2).split("=")(1) +"&"+li(1).split("=")(1) +"&"+li(3).split("=")(1) +"&"+li(4).split("=")(1) +"&"+li(0).split("=")(1)
//    }.foreach(println(_))

//










//    line.map(x=>{
//      x.split(",")
//    }).filter( x => x.length == 7 && !"".eq(x(5)) && !x(5).contains("-"))
//    //id	作者	书籍名	作品简介	出版商	出版日期	类型
//   .map(lines => {
//      val msql = "insert into tb_books(bookid,author,bookname,infor,adress,publishtime,booktype) values("+lines(0)+",'"+lines(1)+"','"+lines(2)+"','"+lines(3)+"','"+lines(4)+"','"+lines(5)+"','"+lines(6)+"');"
//msql
//
//
//
//
//    }).foreach(println)



  }
}
