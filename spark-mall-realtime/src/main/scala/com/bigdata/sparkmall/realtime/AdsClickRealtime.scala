package com.bigdata.sparkmall.realtime

import com.bigdata.sparkmall.common.model.DataModel.AdsLog
import com.bigdata.sparkmall.common.utils.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * 需求五：
  * 实时数据分析：  广告点击量实时统计
  * 每天各地区各城市各广告的点击流量实时统计。
  */
object AdsClickRealtime {

  def main(args: Array[String]): Unit = {

    // 从kafka中获取实时数据
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AdsClickRealtime")

    val ssc = new StreamingContext(conf, Seconds(5))

    val topic = "ads_log"

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc)

    val adsLogDS: DStream[AdsLog] = kafkaDStream.map(record => {
      val log: String = record.value()
      val logs: Array[String] = log.split(" ")

      //timestamp province city userid adid
      AdsLog(logs(0), logs(1), logs(2), logs(3), logs(4))
    })

    adsLogDS.print()

    // (date-province-city-ads, 1)
    val mapDStream: DStream[(String, Int)] = adsLogDS.map(adslog => {

      val date: String = DateUtil.getStringByTimestamp(adslog.timestamp.toLong, "yyyy-MM-dd")
      val key = date + ":" + adslog.province + ":" + adslog.city + ":" + adslog.adid
      (key, 1)
    })

    // (date-province-city-ads, sum)
    val reduceDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_+_)

    // 保存到redis中
    /*reduceDStream.foreachRDD(data=>{

      data.foreachPartition(log=>{
        // 连接redis
        val client: Jedis = RedisUtil.getJedisClient
        val key = "date:area:city:ads"
        for (elem <- log) {
          client.hincrBy(key, elem._1, elem._2)
        }
        client.close()
      })

    })*/

    // 	使用有状态RDD，将数据保存到CP，同时更新Redis
    ssc.sparkContext.setCheckpointDir("cp")
    val updateDStream: DStream[(String, Long)] = reduceDStream.updateStateByKey {
      case (seq, cp) => {
        val sum = cp.getOrElse(0L) + seq.sum
        Option(sum)
      }
    }

    updateDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(message=>{

        val client: Jedis = RedisUtil.getJedisClient

        for ((ads, sum) <- message) {
          client.hset("date:area:city:ads", ads, sum.toString)
        }

        client.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
