package com.cxl.study

/**
  * Created by chenxiaolei on 17/1/17.
  */
object MyModule {
  //声明一个单例对象 即同时声明一个类 和他的唯一实例
  def main(args: Array[String]): Unit = {
    println(add(1, 3))
  }

  def add(x: Int, y: Int): Int = {
    var res = 0
    if (x > y)
     res =  x + y
    else
      res = y - x
    res
  }

}
