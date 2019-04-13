package com.bigdata.sparkmall.realtime

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.bigdata.sparkmall.common.model.DataModel.AdsLog
import com.bigdata.sparkmall.common.utils.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * 实现实时的动态黑名单机制：将每天对某个广告点击超过 100 次的用户拉黑。
  * 注：黑名单保存到redis中。
  * 已加入黑名单的用户不在进行检查。
  */
object BlackMenu {

  def main(args: Array[String]): Unit = {

    // 获取用户点击广告的数据
   // 使用sparkStreamin从kafka中获取数据
   val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BlackMenu")

    val stream = new StreamingContext(conf, Seconds(5))

    val topic = "ads_log"

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, stream)

    val adsLogDF: DStream[AdsLog] = kafkaDStream.map(record => {
      val adsLog: String = record.value()
      val splitAdsLog: Array[String] = adsLog.split(" ")
      //timestamp province city userid adid
      AdsLog(splitAdsLog(0), splitAdsLog(1), splitAdsLog(2), splitAdsLog(3), splitAdsLog(4))
    })

    adsLogDF.print()


     // 判断当前数据中是否含有黑名单数据，如果存在进行过滤
     // 连接redis，从redis中获取数据判断

     val transformDS: DStream[AdsLog] = adsLogDF.transform(log => {

       val client: Jedis = RedisUtil.getJedisClient
       val blacklist: util.Set[String] = client.smembers("blacklist")
       client.close()
       // 将blacklist设置广播变量
       val bcBlacklist: Broadcast[util.Set[String]] = stream.sparkContext.broadcast(blacklist)

       log.filter(data => {
         val black: util.Set[String] = bcBlacklist.value
         !black.contains(data.userid)
       })
     })

     // 在redis中聚合用户点击广告的次数：hash(  date:userid:adid,sumClick )
     transformDS.foreachRDD(adsLog=>{

       adsLog.foreachPartition(logs=>{

         // 连接redis
         val client: Jedis = RedisUtil.getJedisClient

         logs.foreach(log=>{

           val timedate: String = DateUtil.getStringByTimestamp(log.timestamp.toLong, "yyyy-MM-dd")
           val key = "user:click"
           val field = timedate + ":"+log.userid + ":" + log.adid

           client.hincrBy(key, field, 1)

           // 获取聚合后的点击次数进行阈值（100）判断
           val clickSum: String = client.hget(key, field)
           if (20 < clickSum.toLong){
             // 如果点击次数超过阈值，那么会将用户加入redis的黑名单中：set
             client.sadd("blacklist", log.userid)
           }
         })
         client.close()
       })
     })
    stream.start()
    stream.awaitTermination()
}
}
