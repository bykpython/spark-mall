package com.bigdata.sparkmall.realtime

import com.bigdata.sparkmall.common.model.DataModel.AdsLog
import com.bigdata.sparkmall.common.utils.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis

object AdsClickTrendStat {

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

    // 将一定时间范围内的数据当成一个整体数据进行统计
    val windomDStream: DStream[AdsLog] = adsLogDStream.window(Minutes(1), Seconds(10))

    // 如何将窗口中的数据进行分类聚合才是最重要的！！！
    val winMapDStream: DStream[(String, Long)] = windomDStream.map(message => {
      val ts: Long = message.timestamp.toLong

      val tsString = ts / 10000 + "0000"
      (tsString + ":" + message.adid, 1L)
    })

    val reduceDStream: DStream[(String, Long)] = winMapDStream.reduceByKey(_+_)

    val adsDStream: DStream[(String, (String, Long))] = reduceDStream.map {
      case (key, sum) => {
        val keys: Array[String] = key.split(":")
        val ts: String = DateUtil.getStringByTimestamp(keys(0).toLong, "hh:mm:ss")
        (keys(1), (ts, sum))
      }
    }

    val sortDStream: DStream[(String, List[(String, Long)])] = adsDStream.groupByKey().mapValues(iter => {
      iter.toList.sortWith {
        case (left, right) => {
          left._1 < right._1
        }
      }
    })

    sortDStream.print()

    sortDStream.foreachRDD(rdd=>{

      rdd.foreachPartition(trend=>{

        // 连接redis
        val client: Jedis = RedisUtil.getJedisClient
        trend.foreach{
          case (ads, timeTrend)=>{
            val timeMap: Map[String, Long] = timeTrend.toMap
            import org.json4s.JsonDSL._
            val jsonStr: String = JsonMethods.compact(JsonMethods.render(timeMap))
            client.hset("time:advert:click:trend", ads, jsonStr)
          }
        }
        client.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
