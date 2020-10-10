package com.atguigu.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.ProviceInfo
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName: ProvinceInfo
 * @Description: TODO 熊kafka里面读取省份信息保存hbase
 * @Author: VmPerson
 * @Date: 2020/9/18  19:42
 * @Version: 1.0
 */
object ProvinceInfoApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("ProvinceInfoApp").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(10))
    var topic = "ods_base_province"
    var groupId = "ods_province_group"

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

    import org.apache.phoenix.spark._
    ds.foreachRDD(elem => {
      val provincerdd: RDD[ProviceInfo] = elem.map(rdd => {
        val objStr: String = rdd.value()
        val proviceInfo: ProviceInfo = JSON.parseObject(objStr, classOf[ProviceInfo])
        proviceInfo
      })
      provincerdd.saveToPhoenix("GMALL0421_PROVINCE_INFO",
        Seq("ID","NAME","AREA_CODE","ISO_CODE"),
        new Configuration,
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
      OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
    })


    ssc.start()
    ssc.awaitTermination()
  }


}
