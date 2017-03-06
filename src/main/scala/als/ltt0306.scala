package als

import breeze.linalg.rank
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by chenxiaolei on 2017/3/6.
  */
object ltt0306 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)
    //设置运行环境 spark 内存计算框架 内存计算快 没有io 没有网络操作 计算 100倍以上
    val conf = new SparkConf().setAppName("book_als").setMaster("local[2]")
    //建立入口
    val sc = new SparkContext(conf)
    //装载用户评分 需要做推荐的用户list
    val myRatings: Seq[Rating] = loadRatings("/Users/chenxiaolei/ltt_book/GradeData.base")
    val training = sc.parallelize(myRatings, 2)
    val ranks = 10
    val lambdas = 0.02
    val numIters = 20
    //查看矩阵条数
    val numTraining = 80000
      val model = ALS.train(training, ranks, numIters, lambdas)
      //计算rmse
      val trainingRmse = computeRmse(model, training, numTraining)
      println("RMSE(training) = " +
        trainingRmse + " 模型的 trained and rank = "
      +ranks + ",lambda = " + lambdas + ",and numIter = " + numIters + "."
      )



  }
  def loadRatings(path: String): Seq[Rating] = {
    val lines = Source.fromFile(path).getLines()
    val ratings: Iterator[Rating] = lines.map {
      line =>
        val fields = line.split("\t")
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.filter(_.rating > 0.0)
    if (ratings.isEmpty) {
      sys.error("没有数据")
    } else {
      ratings.toSeq
    }
  }
  /** 校验集预测数据和实际数据之间的均方根误差 **/
  def computeRmse(model:MatrixFactorizationModel,data:RDD[Rating],n:Long):Double = {

    val predictions:RDD[Rating] = model.predict((data.map(x => (x.user,x.product))))
    val predictionsAndRatings = predictions.map{ x =>((x.user,x.product),x.rating)}
      .join(data.map(x => ((x.user,x.product),x.rating))).values
    math.sqrt(predictionsAndRatings.map( x => (x._1 - x._2) * (x._1 - x._2)).reduce(_+_)/n)
  }
}
