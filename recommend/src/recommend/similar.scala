

/**
  * Created by shaohui on 2016/12/1 0001.
  */
package recommend

import org.apache.spark.rdd.RDD

import scala.math._

/**
  * �û�����.
  * @param userid �û�
  * @param itemid ������Ʒ
  * @param pref ����
  */
case class ItemPref(userid: String,itemid: String,pref: Double) extends Serializable
/**
  * �û��Ƽ�.
  * @param userid �û�
  * @param itemid �Ƽ���Ʒ
  * @param pref ����
  */
case class UserRecomm(
                       userid: String,
                       itemid: String,
                       pref: Double) extends Serializable
/**
  * ���ƶ�.
  * @param itemid1 ��Ʒ
  * @param itemid2 ��Ʒ
  * @param similar ���ƶ�
  */
case class ItemSimi(
                     itemid1: String,
                     itemid2: String,
                     similar: Double) extends Serializable

/**
  * ���ƶȼ���.
  * ֧�֣�ͬ�����ƶȡ�ŷ�Ͼ������ƶȡ��������ƶ�
  *
  */
class ItemSimilarity extends Serializable {

  /**
    * ���ƶȼ���.
    * @param user_rdd �û�����
    * @param stype �������ƶȹ�ʽ
    * @param RDD[ItemSimi] ������Ʒ���ƶ�
    *
    */
  def Similarity(user_rdd: RDD[ItemPref], stype: String): (RDD[ItemSimi]) = {
    val simil_rdd = stype match {
      case "cooccurrence" =>
        ItemSimilarity.CooccurrenceSimilarity(user_rdd)
      case "cosine" =>
        ItemSimilarity.CosineSimilarity(user_rdd)
      case "euclidean" =>
        ItemSimilarity.EuclideanDistanceSimilarity(user_rdd)
      case _ =>
        ItemSimilarity.CooccurrenceSimilarity(user_rdd)
    }
    simil_rdd
  }

}

object ItemSimilarity {

  /**
    * ͬ�����ƶȾ������.
    * w(i,j) = N(i)��N(j)/sqrt(N(i)*N(j))
    * @param user_rdd �û�����
    * @param RDD[ItemSimi] ������Ʒ���ƶ�
    *
    */
  def CooccurrenceSimilarity(user_rdd: RDD[ItemPref]): (RDD[ItemSimi]) = {
    // 0 ������׼��
    val user_rdd1 = user_rdd.map(f => (f.userid, f.itemid, f.pref))
    val user_rdd2 = user_rdd1.map(f => (f._1, f._2))
    // 1 (�û�����Ʒ) �ѿ����� (�û�����Ʒ) => ��Ʒ:��Ʒ���
    val user_rdd3 = user_rdd2.join(user_rdd2)
    val user_rdd4 = user_rdd3.map(f => (f._2, 1))
    // 2 ��Ʒ:��Ʒ:Ƶ��
    val user_rdd5 = user_rdd4.reduceByKey((x, y) => x + y)
    // 3 �ԽǾ���
    val user_rdd6 = user_rdd5.filter(f => f._1._1 == f._1._2)
    // 4 �ǶԽǾ���
    val user_rdd7 = user_rdd5.filter(f => f._1._1 != f._1._2)
    // 5 ����ͬ�����ƶȣ���Ʒ1����Ʒ2��ͬ��Ƶ�Σ�
    val user_rdd8 = user_rdd7.map(f => (f._1._1, (f._1._1, f._1._2, f._2))).
      join(user_rdd6.map(f => (f._1._1, f._2)))
    val user_rdd9 = user_rdd8.map(f => (f._2._1._2, (f._2._1._1,
      f._2._1._2, f._2._1._3, f._2._2)))
    val user_rdd10 = user_rdd9.join(user_rdd6.map(f => (f._1._1, f._2)))
    val user_rdd11 = user_rdd10.map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))
    val user_rdd12 = user_rdd11.map(f => (f._1, f._2, f._3 / sqrt(f._4 * f._5)))
    // 6 �������
    user_rdd12.map(f => ItemSimi(f._1, f._2, f._3))
  }

  /**
    * �������ƶȾ������.
    * T(x,y) = ��x(i)y(i) / sqrt(��(x(i)*x(i)) * ��(y(i)*y(i)))
    * @param user_rdd �û�����
    * @param RDD[ItemSimi] ������Ʒ���ƶ�
    *
    */
  def CosineSimilarity(user_rdd: RDD[ItemPref]): (RDD[ItemSimi]) = {
    // 0 ������׼��
    val user_rdd1 = user_rdd.map(f => (f.userid, f.itemid, f.pref))
    val user_rdd2 = user_rdd1.map(f => (f._1, (f._2, f._3)))
    // 1 (�û�,��Ʒ,����) �ѿ����� (�û�,��Ʒ,����) => ����Ʒ1,��Ʒ2,����1,����2�����
    val user_rdd3 = user_rdd2.join(user_rdd2)
    val user_rdd4 = user_rdd3.map(f => ((f._2._1._1, f._2._2._1), (f._2._1._2, f._2._2._2)))
    // 2 ����Ʒ1,��Ʒ2,����1,����2����� => ����Ʒ1,��Ʒ2,����1*����2�� ��� ���ۼ�
    val user_rdd5 = user_rdd4.map(f => (f._1, f._2._1 * f._2._2)).reduceByKey(_ + _)
    // 3 �ԽǾ���
    val user_rdd6 = user_rdd5.filter(f => f._1._1 == f._1._2)
    // 4 �ǶԽǾ���
    val user_rdd7 = user_rdd5.filter(f => f._1._1 != f._1._2)
    // 5 �������ƶ�
    val user_rdd8 = user_rdd7.map(f => (f._1._1, (f._1._1, f._1._2, f._2))).
      join(user_rdd6.map(f => (f._1._1, f._2)))
    val user_rdd9 = user_rdd8.map(f => (f._2._1._2, (f._2._1._1,
      f._2._1._2, f._2._1._3, f._2._2)))
    val user_rdd10 = user_rdd9.join(user_rdd6.map(f => (f._1._1, f._2)))
    val user_rdd11 = user_rdd10.map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))
    val user_rdd12 = user_rdd11.map(f => (f._1, f._2, f._3 / sqrt(f._4 * f._5)))
    // 6 �������
    user_rdd12.map(f => ItemSimi(f._1, f._2, f._3))
  }

  /**
    * ŷ�Ͼ������ƶȾ������.
    * d(x, y) = sqrt(��((x(i)-y(i)) * (x(i)-y(i))))
    * sim(x, y) = n / (1 + d(x, y))
    * @param user_rdd �û�����
    * @param RDD[ItemSimi] ������Ʒ���ƶ�
    *
    */
  def EuclideanDistanceSimilarity(user_rdd: RDD[ItemPref]): (RDD[ItemSimi]) = {
    // 0 ������׼��
    val user_rdd1 = user_rdd.map(f => (f.userid, f.itemid, f.pref))
    val user_rdd2 = user_rdd1.map(f => (f._1, (f._2, f._3)))
    // 1 (�û�,��Ʒ,����) �ѿ����� (�û�,��Ʒ,����) => ����Ʒ1,��Ʒ2,����1,����2�����
    val user_rdd3 = user_rdd2 join user_rdd2
    val user_rdd4 = user_rdd3.map(f => ((f._2._1._1, f._2._2._1), (f._2._1._2, f._2._2._2)))
    // 2 ����Ʒ1,��Ʒ2,����1,����2����� => ����Ʒ1,��Ʒ2,����1-����2�� ��� ���ۼ�
    val user_rdd5 = user_rdd4.map(f => (f._1, (f._2._1 - f._2._2) * (f._2._1 - f._2._2))).reduceByKey(_ + _)
    // 3 ����Ʒ1,��Ʒ2,����1,����2����� => ����Ʒ1,��Ʒ2,1�� ��� ���ۼ�    �����ص���
    val user_rdd6 = user_rdd4.map(f => (f._1, 1)).reduceByKey(_ + _)
    // 4 �ǶԽǾ���
    val user_rdd7 = user_rdd5.filter(f => f._1._1 != f._1._2)
    // 5 �������ƶ�
    val user_rdd8 = user_rdd7.join(user_rdd6)
    val user_rdd9 = user_rdd8.map(f => (f._1._1, f._1._2, f._2._2 / (1 + sqrt(f._2._1))))
    // 6 �������
    user_rdd9.map(f => ItemSimi(f._1, f._2, f._3))
  }

}



