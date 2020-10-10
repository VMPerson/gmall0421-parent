package com.atguigu.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.realtime.utils.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName: ODSCanal
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/16  18:50
 * @Version: 1.0
 */
object ODSCanal {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("ODSCanal").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    var topic = "gmall0421_db_c"
    var groupId = "db_canal"

    //先从redis里面获取偏移量
    val partitionToLong: Map[TopicPartition, Long] = OffsetManagerUtil.getOffSet(topic, groupId)
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (partitionToLong != null && partitionToLong.size > 0) {
      recordDStream = MyKafkaUtil.collectKafkaStream(topic, ssc, groupId, partitionToLong)
    } else {
      recordDStream = MyKafkaUtil.collectKafkaStream(topic, ssc, groupId)
    }
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    //现在我们获取到的是ConsumerRecoder，我们要记录offsetRanges
    val ds: DStream[ConsumerRecord[String, String]] = recordDStream.transform(elem => {
      offsetRanges = elem.asInstanceOf[HasOffsetRanges].offsetRanges
      elem
    })
    //开始处理ConsumerRecoder
    val jsObj: DStream[JSONObject] = ds.map(elem => {
      val jsonStr: String = elem.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonStr)
      jsonObj
    })

    jsObj.foreachRDD(elem => {
      elem.foreach(rdd => {
        val tableName: String = rdd.getString("table")
        var topicName = "ods_" + tableName
        val optType: String = rdd.getString("type")
        val jSONArray: JSONArray = rdd.getJSONArray("data")
        if (jSONArray != null && jSONArray.size() > 0 && optType != "DELETE") {
          import scala.collection.JavaConverters._
          for (job <- jSONArray.asScala) {
            MyKafkaSink.send(topicName, job.toString)
          }
        }
      })
      OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()

  }


}
