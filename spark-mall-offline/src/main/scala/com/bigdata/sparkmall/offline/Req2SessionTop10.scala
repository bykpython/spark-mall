package com.bigdata.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.bigdata.sparkmall.common.model.DataModel.{CategoryTop10, UserVisitAction}
import com.bigdata.sparkmall.common.utils.{ConfigurationUtil, SparkMallUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.{immutable, mutable}

/**
  * 对于排名前 10 的品类，分别获取其点击次数排名前 10 的 sessionId
  */
object Req2SessionTop10 {

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

    if (SparkMallUtil.isNotEmptyValue(startDate)) {
      sqlBuilder.append(" and action_time >= '").append(startDate).append("'")
    }
    if (SparkMallUtil.isNotEmptyValue(endDate)) {
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

    actionRDD.foreach(data => {
      if (data.click_category_id != -1) {
        myAcc.add(data.click_category_id + "_click")
      } else if (data.order_category_ids != null) {
        val orderIds: Array[String] = data.order_category_ids.split(",")
        for (elem <- orderIds) {
          myAcc.add(elem + "_order")
        }
      } else if (data.pay_category_ids != null) {
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

    // 根据热门top10的品类过滤用户行为日志
    val top10Category: List[String] = categoryTop10.map(_.category_id)

    val clickCategoryRDD: RDD[UserVisitAction] = actionRDD.filter(data => {
      top10Category.contains(data.click_category_id + "")
    })

    // 根据品类进行聚合
    val reduceBycategory: RDD[(String, Int)] = clickCategoryRDD.map(data=>(data.click_category_id + "-" + data.session_id, 1)).reduceByKey(_+_)

    // （category，（session， sum））
    val mapCategorySession: RDD[(String, (String, Int))] = reduceBycategory.map {
      case (doubleKey, sum) => {
        val keys: Array[String] = doubleKey.split("-")
        (keys(0), (keys(1), sum))
      }
    }

    // 按照category进行分组（category， （（session， sum），（session， sum），...））
    val groupByCategorySession: RDD[(String, Iterable[(String, Int)])] = mapCategorySession.groupByKey()

    // 将每一个category中的session进行倒序排序，取前十
    val categoryTop10SessionTop10: RDD[(String, List[(String, Int)])] = groupByCategorySession.mapValues(iter => {
      iter.toList.sortWith {
        case (left, right) => {
          if (left._2 > right._2){
            true
          }else{
            false
          }
        }
      }.take(10)
    })

    // 将数据保存到mysql中
    val driver: String = ConfigurationUtil.getValueByKey("jdbc.driver.class")
    val url: String = ConfigurationUtil.getValueByKey("jdbc.url")
    val user: String = ConfigurationUtil.getValueByKey("jdbc.user")
    val password: String = ConfigurationUtil.getValueByKey("jdbc.password")

    categoryTop10SessionTop10.foreachPartition(data=>{

      // 连接数据库
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, user, password)
      val insertSql = "insert into category_top10_session_count values(?, ?, ?, ?)"

      data.foreach{
        case (category, list)=>{

          val pst: PreparedStatement = connection.prepareStatement(insertSql)
          for (elem <- list) {
            pst.setObject(1, taskId)
            pst.setObject(2, category)
            pst.setObject(3, elem._1)
            pst.setObject(4, elem._2)

            pst.executeUpdate()
          }
          pst.close()
        }
      }
      connection.close()
    })

    spark.stop()
  }

}
