package com.bigdata.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.bigdata.sparkmall.common.model.DataModel.UserVisitAction
import com.bigdata.sparkmall.common.utils.{ConfigurationUtil, SparkMallUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable


/**
  * 需求三；页面单跳转化率统计
  */
object Req3PageJumpRate {

  def main(args: Array[String]): Unit = {

    // 获取Hive中保存日志数据
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req3PageJumpRate")

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

    // 将日志数据根据session进行分组排序，获取同一个session中的页面跳转路径
    val groupByRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(data => {
      data.session_id
    })

    // 将页面跳转路径形成拉链效果  AB.BC
    val zipPageIds: RDD[(String, List[(Long, Long)])] = groupByRDD.mapValues(iter => {
      val sortVisitAction: List[UserVisitAction] = iter.toList.sortWith {
        case (left, right) => {
          left.action_time < right.action_time
        }
      }

      val pageIds: List[Long] = sortVisitAction.map(_.page_id)
      pageIds.zip(pageIds.tail)
    })

    val newMapPages: RDD[List[String]] = zipPageIds.map {
      case (session, list) => {
        list.map(tup => {
          tup._1 + "-" + tup._2
        })
      }
    }

    val finalPages: RDD[String] = newMapPages.flatMap(x=>x)

    // 按条件过滤掉不需要的页面跳转
    val pages: String = ConfigurationUtil.getCondValue("targetPageFlow")
    val pageids: Array[String] = pages.split(",")
    val condZipPageIds: Array[(String, String)] = pageids.zip(pageids.tail)
    val condZip: Array[String] = condZipPageIds.map(tup=>tup._1 + "-" + tup._2)

    val filterPageIds: RDD[String] = finalPages.filter(pageids => {
      condZip.contains(pageids)
    })

    // 统计拉链后的数据点击总次数（A）
    val reduceByKeyPageJump: RDD[(String, Int)] = filterPageIds.map(pageids=>(pageids, 1)).reduceByKey(_+_)

    // 将符合条件的日志数据根据页面ID进行分组聚合（B）
    val filterLogRDD: RDD[UserVisitAction] = actionRDD.filter(data => {
      pageids.contains(data.page_id + "")
    })

    val reduceByKeyLogPage: RDD[(Long, Int)] = filterLogRDD.map(log=>(log.page_id, 1)).reduceByKey(_+_)
    val logPage: Map[Long, Int] = reduceByKeyLogPage.collect().toMap

    // 将计算结果A / B,获取转换率
    // 将转换率通过JDBC保存到Mysql中

    val driver: String = ConfigurationUtil.getValueByKey("jdbc.driver.class")
    val url: String = ConfigurationUtil.getValueByKey("jdbc.url")
    val user: String = ConfigurationUtil.getValueByKey("jdbc.user")
    val password: String = ConfigurationUtil.getValueByKey("jdbc.password")

    val taskId: String = UUID.randomUUID().toString
    reduceByKeyPageJump.foreachPartition(data=>{

      // 连接数据库
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, user, password)
      val insertSql = "insert into pagejumprate values(?, ?, ?)"
      val pst: PreparedStatement = connection.prepareStatement(insertSql)

      data.foreach{
        case (jump, sum)=>{
          val perPage: String = jump.split("-")(0)
          val pageSum = logPage(perPage.toInt)
          val rate = sum.toDouble / pageSum.toDouble * 100
          pst.setObject(1, taskId)
          pst.setObject(2, jump)
          pst.setObject(3, rate)

          pst.executeUpdate()
        }
      }
      pst.close()
      connection.close()
    })

  }

}
