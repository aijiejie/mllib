package recommend

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shaohui on 2016/12/1 0001.
  */
object text {
  def main(args: Array[String]) {

    //0 ????Spark????
    val conf = new SparkConf().setAppName("协同过滤")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    //1 ???????????
    val data_path = "hdfs:///ceshi/itemsim"
    val data = sc.textFile(data_path)
    val userdata = data.map(_.split(" ")).map(f => ItemPref(f(0), f(1), f(2).toDouble)).cache()

    //2 ???????
    val mysimil = new ItemSimilarity()
    val simil_rdd1 = mysimil.Similarity(userdata, "cooccurrence")
    val recommd = new RecommendedItem
    val recommd_rdd1 = recommd.Recommend(simil_rdd1, userdata, 10)

    //3 ??????
    println(s"物品相似度矩阵: ${simil_rdd1.count()}")
    simil_rdd1.collect().foreach { ItemSimi =>
      println(ItemSimi.itemid1 + ", " + ItemSimi.itemid2 + ", " + ItemSimi.similar)
    }
    println(s"用户推荐列表: ${recommd_rdd1.count()}")
    recommd_rdd1.collect().foreach { UserRecomm =>
      println(UserRecomm.userid + ", " + UserRecomm.itemid + ", " + UserRecomm.pref)
    }

  }
}
