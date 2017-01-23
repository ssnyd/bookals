package als

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

import scala.io.Source


/**
  * Created by chenxiaolei on 17/1/18.
  */
object BookALS {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("book_als").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //装载用户评分
    val myRatings = loadRatings("/Users/chenxiaolei/ltt_book/personalRatings.txt")
    val myRatingsRDD = sc.parallelize(myRatings, 1)

    //样本数据目录
    val bookHomeDir = "/Users/chenxiaolei/ltt_book/"
    //装载样本评分数据 其中 最后一列的时间 取除10的余数作为key,Rating 为值,即（Int,Rating）
    val ratings = sc.textFile(bookHomeDir + "/ratings.dat").map {
      line =>
        val fields = line.split("::")
        (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }
    //装载书籍目录对照表 （书籍id -> book名称）
    val books = sc.textFile(bookHomeDir + "/books").map {
      line =>
        val fields = line.split("&&")
        // format: (bookId, bookName)
        (fields(0).toInt, fields(2))
    }.collect().toMap
    val numRatings = ratings.count()
    val numUsers = ratings.map(_._2.user).distinct().count()
    val numBooks = ratings.map(_._2.product).distinct().count()
    println("评分总数 " + numRatings + " 来自 " + numUsers + " 个用户对 " + numBooks + " 本书")
    //将评分样表以key分成3个部分 其中
    // 60％ 用于训练 并加入用户评分
    // 20％ 用于校验
    // 20% 用于测试
    // 该数据要多次用到 故cache到内存
    val numPartitions = 4
    val training = ratings.filter(x => x._1 < 6).values.union(myRatingsRDD).repartition(numPartitions).persist()
    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8).values.repartition(numPartitions).persist()
    val test = ratings.filter(x => x._1 >= 8).values.persist()
    //查看数据量
    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()
    println("训练数据: " + numTraining + " 校验数据: " + numValidation + " 测试数据: " + numTest)
    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIters = List(10, 20)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1

    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation, numValidation)
      println("RMSE(validation) = " + validationRmse + " 模型的 trained and rank = "
        + rank + ",lambda = " + lambda + ",and numIter = " + numIter + ".")

      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }
    //用最佳模型预测测试集的评分，并计算和实际评分之间的均方根误差（RMSE）
    val testRmse = computeRmse(bestModel.get, test, numTest)
    println("最佳训练 rank = " + bestRank + " and lambda = " + bestLambda
      + ", and numIter = " + bestNumIter + ", and 测试集RMSE 是 " + testRmse + ".")

    //create a naive baseline and compare it with the best model
    val meanRating = training.union(validation).map(_.rating).mean
    val baselineRmse = math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).reduce(_ + _) / numTest)
    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
    println("最佳的模型是" + "%1.2f".format(improvement) + "%.")

    //推荐前十部最感兴趣的图书，注意要剔除用户已经评分的图书
    val myRatedMovieIds = myRatings.map(_.product).toSet
    val candidates = sc.parallelize(books.keys.filter(!myRatedMovieIds.contains(_)).toSeq)
    val recommendations = bestModel.get
      .predict(candidates.map((0, _)))
      .collect
      .sortBy(-_.rating)
      .take(10)
    var i = 1
    println("推荐的图书是:")
    recommendations.foreach { r =>
      println("%2d".format(i) + ": " + books(r.product))
      i += 1
    }
    sc.stop()
  }


  def loadRatings(path: String): Seq[Rating] = {
    val lines = Source.fromFile(path).getLines()
    val ratings = lines.map {
      line =>
        val fields = line.split("::")
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
