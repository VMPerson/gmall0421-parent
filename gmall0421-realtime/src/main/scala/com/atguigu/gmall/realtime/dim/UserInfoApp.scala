package com.atguigu.gmall.realtime.dim

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.UserInfo
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName: UserInfoApp
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/18  20:53
 * @Version: 1.0
 */
object UserInfoApp {

  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(10))
    var topic = "ods_user_info"
    var groupId = "ods_userinfo_group"

    //读取偏移量
    val partitionToLong: Map[TopicPartition, Long] = OffsetManagerUtil.getOffSet(topic, groupId)
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (partitionToLong != null && partitionToLong.size > 0) {
      recordDStream = MyKafkaUtil.collectKafkaStream(topic, ssc, groupId, partitionToLong)
    } else {
      recordDStream = MyKafkaUtil.collectKafkaStream(topic, ssc, groupId)
    }
    //记录偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val ds: DStream[ConsumerRecord[String, String]] = recordDStream.transform(elem => {
      offsetRanges = elem.asInstanceOf[HasOffsetRanges].offsetRanges
      elem
    })

    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val infoDStream: DStream[UserInfo] = ds.map(elem => {
      val jsonStr: String = elem.value()
      val userInfo: UserInfo = JSON.parseObject(jsonStr, classOf[UserInfo])
      val birthday: String = userInfo.birthday
      val date: Date = format.parse(birthday)
      val mils: Long = System.currentTimeMillis() - date.getTime
      val years: Long = mils / 1000L / 60L / 60L / 24L / 365L
      if (years < 20) {
        userInfo.age_group = "20岁及以下"
      } else if (years > 30) {
        userInfo.age_group = "30岁以上"
      } else {
        userInfo.age_group = "21岁到30岁"
      }
      if (userInfo.gender == "M") {
        userInfo.gender_name = "男"
      } else {
        userInfo.gender_name = "女"
      }
      userInfo
    })


    import org.apache.phoenix.spark._
    infoDStream.foreachRDD(elem => {
      elem.saveToPhoenix("GMALL0421_USER_INFO",
        Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER", "AGE_GROUP", "GENDER_NAME")
        , new Configuration, Some("hadoop102,hadoop103,hadoop104:2181"))
      OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()


  }


}
