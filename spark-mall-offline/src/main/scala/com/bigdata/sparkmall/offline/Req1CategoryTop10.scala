package com.bigdata.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.bigdata.sparkmall.common.model.DataModel.{CategoryTop10, UserVisitAction}
import com.bigdata.sparkmall.common.utils.{ConfigurationUtil, SparkMallUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

/**
  * 需求一：Top10 热门品类
  * 获取点击、下单和支付数量排名前 10 的品类
  */
object Req1CategoryTop10 {

  def main(args: Array[String]): Unit = {

    // 创建spark配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("offlineutils")

    // 创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 从hive中获取数据
    spark.sql("use " + ConfigurationUtil.getValueByKey("hive.database"))

    val selectSql = "select * from user_visit_action where 1 = 1"
    val sqlBuilder = new StringBuilder(selectSql)

    // 获取查询sql所需要的条件
    val startDate: String = ConfigurationUtil.getCondValue("startDate")
    val endDate: String = ConfigurationUtil.getCondValue("endDate")

    if (SparkMallUtil.isNotEmptyValue(startDate)){
      sqlBuilder.append(" and action_time >= '").append(startDate).append("'")
    }
    if (SparkMallUtil.isNotEmptyValue(endDate)){
      sqlBuilder.append(" and action_time <= '").append(endDate).append("'")
    }

    // 执行sql语句
    val actionLogDF: DataFrame = spark.sql(sqlBuilder.toString())

    // 将dataframe转换为rdd
    val actionRDD: RDD[UserVisitAction] = actionLogDF.as[UserVisitAction].rdd

    // 将品类聚合成不同属性的数据（category, sumClick）,(category, sumOrder),(Category, sumPay)
        // 使用累加器累加聚合数据（推荐使用）
    val myAcc = new MyAccumulator
    spark.sparkContext.register(myAcc)

    actionRDD.foreach(data=>{
      if (data.click_category_id != -1){
        myAcc.add(data.click_category_id + "_click")
      }else if (data.order_category_ids != null){
        val orderIds: Array[String] = data.order_category_ids.split(",")
        for (elem <- orderIds) {
          myAcc.add(elem + "_order")
        }
      }else if (data.pay_category_ids != null){
        val payIds: Array[String] = data.pay_category_ids.split(",")
        for (elem <- payIds) {
          myAcc.add(elem + "_pay")
        }
      }
    })

    val accVaule: mutable.HashMap[String, Long] = myAcc.value

    // 将聚合的数据融合在一起（category, (sumClick, sumOrder, sumPay)）
    val groupByValue: Map[String, mutable.HashMap[String, Long]] = accVaule.groupBy {
      case (category, click) => {
        category.split("_")(0)
      }
    }

    val taskId: String = UUID.randomUUID().toString
    val mapCategory: immutable.Iterable[CategoryTop10] = groupByValue.map {
      case (category, hashmap) => {
        CategoryTop10(taskId,
          category,
          hashmap.getOrElse(category + "_click", 0L),
          hashmap.getOrElse(category + "_order", 0L),
          hashmap.getOrElse(category + "_pay", 0L))
      }
    }

    // 将聚合的数据根据要求进行倒序排列，取前10条
    val categoryTop10: List[CategoryTop10] = mapCategory.toList.sortWith {
      case (left, right) => {
        if (left.click_count < right.click_count) {
          false
        } else if (left.click_count == right.click_count) {
          if (left.order_count < right.order_count) {
            false
          } else if (left.order_count == right.order_count) {
            left.pay_count > right.pay_count
          }
          else {
            true
          }
        }
        else {
          true
        }
      }
    }.take(10)

    // 将分析结果保存到MySQL中
    // 将统计结果使用JDBC存储到Mysql中
    val driver: String = ConfigurationUtil.getValueByKey("jdbc.driver.class")
    val url: String = ConfigurationUtil.getValueByKey("jdbc.url")
    val user: String = ConfigurationUtil.getValueByKey("jdbc.user")
    val password: String = ConfigurationUtil.getValueByKey("jdbc.password")

    Class.forName(driver)
    val connection: Connection = DriverManager.getConnection(url, user, password)
    val insertSql = "insert into category_top10 values(?, ?, ?, ?, ?)"
    categoryTop10.foreach(top10=>{

      val pst: PreparedStatement = connection.prepareStatement(insertSql)
      pst.setObject(1, top10.taskId)
      pst.setObject(2, top10.category_id)
      pst.setObject(3, top10.click_count)
      pst.setObject(4, top10.order_count)
      pst.setObject(5, top10.pay_count)

      pst.executeUpdate()
      pst.close()
    })

    spark.stop()

  }

}

class MyAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]]{

  private var myMap = new mutable.HashMap[String, Long]()

  // 判断是否为初始状态
  override def isZero: Boolean = {
    myMap.isEmpty
  }

  // 复制累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new MyAccumulator
  }

  // 重置累加器
  override def reset(): Unit = {
    myMap.clear()
  }

  // 将累加器和输入数据进行叠加
  override def add(v: String): Unit = {
    myMap(v) = myMap.getOrElse(v, 0L) + 1L
  }

  // 合并累加器
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {

    myMap = myMap.foldLeft(other.value){
      case (hashMap, (key, value))=>{
        hashMap(key) = hashMap.getOrElse(key, 0L) + value
        hashMap
      }
    }
  }

  // 返回输出值
  override def value: mutable.HashMap[String, Long] = {
    return myMap
  }
}