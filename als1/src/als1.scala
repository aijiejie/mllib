import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}
import scala.sys.process._

/**
  * Created by shaohui on 2016/12/11 0011.
  */
object als1 {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("als")
    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs:///ceshi/als1/test.data")
    val ratings = data.map(_.split(",") match { case Array(user, item, rate) => Rating(user.toInt, item.toInt, rate.toDouble) })

    val rank = 10
    val numTterations = 20
    val model = ALS.train(ratings, rank, numTterations, 0.01)


    val userProduct = ratings.map {
      case Rating(user, product, rate) => (user, product)
    }
    val predictions = model.predict(userProduct).map {
      case Rating(user, product, rate) => ((user, product), rate)
    }
    val rateandpreds = ratings.map {
      case Rating(user, product, rate) => ((user, product), rate)
    }.join(predictions)
    val MSE = rateandpreds.map {
      case ((user, product), (r1, r2)) => val err = (r1 - r2)
        err * err
    }.mean()



    "hadoop fs -rm -r /ceshi/als1/data".!

    /*
    保存数据，先按用户排序，然后重新分区确保目标目录中只生成一个文件（用户，产品，评分，预测）
    * */
    rateandpreds.sortByKey().repartition(1).sortBy(_._1).map({
      case ((user, product), (rate, pred)) => (user + "," + product + "," + rate + "," + pred)}).saveAsTextFile("hdfs:///ceshi/als1/data")


    /*
    对预测的评分结果按用户进行分组并按评分倒排序
    * */
    predictions.map { case ((user, product), rate) =>
      (user, (product, rate))
    }.groupByKey().map{case (user_id,list)=>
      (user_id,list.toList.sortBy {case (goods_id,rate)=> - rate})
    }.saveAsTextFile("hdfs:///ceshi/als1/data1")



    println("Mean Squared Error = " + MSE)



    "hadoop fs -rm -r /user/hadoop/myModePath".!
    val ModelPath = "hdfs:///ceshi/als1/model1" //模型保存地址
    model.save(sc, "myModePath")
    val sameModel = MatrixFactorizationModel.load(sc, "myModePath")
  }

}
