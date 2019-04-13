package com.bigdata.sparkmall.realtime

import com.bigdata.sparkmall.common.model.DataModel.AdsLog
import com.bigdata.sparkmall.common.utils.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis

/**
  * 需求六：
  * 实时数据分析：  每天各地区 top3 热门广告
  * 每天各地区 top3 热门广告
  */
object HotAdsTop3 {

  def main(args: Array[String]): Unit = {

    // 从kafka中获取数据
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotAdsTop3")

    val ssc = new StreamingContext(conf, Seconds(5))

    val topic = "ads_log"

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc)

    val adsLogDStream: DStream[AdsLog] = kafkaDStream.map(record => {
      val adslog: String = record.value()
      val logs: Array[String] = adslog.split(" ")

      //timestamp province city userid adid
      AdsLog(logs(0), logs(1), logs(2), logs(3), logs(4))
    })

    // 转换结构 -->(date-area-ads, 1)
    val mapAdsDStream: DStream[(String, Int)] = adsLogDStream.map(ads => {
      val date: String = DateUtil.getStringByTimestamp(ads.timestamp.toLong, "yyyy-MM-dd")
      val key = date + ":" + ads.province + ":" + ads.adid

      (key, 1)
    })

    // 进行聚合
    val reduceByKeyDStream: DStream[(String, Int)] = mapAdsDStream.reduceByKey(_ + _)

    val groupBykeyDS: DStream[(String, Iterable[(String, Int)])] = reduceByKeyDStream.map {
      case (key, sum) => {
        val keys: Array[String] = key.split(":")
        (keys(0) + ":" + keys(1), (keys(2), sum))
      }
    }.groupByKey()

    val hotAdsTop3: DStream[(String, List[(String, Int)])] = groupBykeyDS.mapValues(iter => {
      iter.toList.sortWith {
        case (left, right) => {
          left._2 > right._2
        }
      }.take(3)
    })

    hotAdsTop3.print()

    hotAdsTop3.foreachRDD(ads=>{
      ads.foreachPartition(ad=>{

        // 连接rdis
        val client: Jedis = RedisUtil.getJedisClient

        ad.foreach{
          case (key, list)=>{
            val keys: Array[String] = key.split(":")
            val hashKey = "top3_ads_per_day:" + keys(0)
            val field = keys(1)

            import org.json4s.JsonDSL._
            val map: Map[String, Int] = list.toMap
            val jsonStr: String = JsonMethods.compact(JsonMethods.render(map))

            client.hset(hashKey, field, jsonStr)
          }
        }
        client.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
