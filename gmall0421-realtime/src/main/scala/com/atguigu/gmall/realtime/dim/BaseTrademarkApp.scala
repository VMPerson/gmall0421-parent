package com.atguigu.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.BaseTrademark
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName: BaseTrademarkApp
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/20  19:25
 * @Version: 1.0
 */
object BaseTrademarkApp {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("BaseTrademarkApp")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    var topic = "ods_base_trademark"
    var group = "dim_base_trademark_group"


    val partitionToLong: Map[TopicPartition, Long] = OffsetManagerUtil.getOffSet(topic, group)
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (partitionToLong != null && partitionToLong.size > 0) {
      recordDStream = MyKafkaUtil.collectKafkaStream(topic, ssc, group, partitionToLong)
    } else {
      recordDStream = MyKafkaUtil.collectKafkaStream(topic, ssc, group)
    }

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val ds: DStream[ConsumerRecord[String, String]] = recordDStream.transform(elem => {
      offsetRanges = elem.asInstanceOf[HasOffsetRanges].offsetRanges
      elem
    })


    val tradeMarkDStream: DStream[BaseTrademark] = ds.map(rec => {
      val obj: String = rec.value()
      val trademark: BaseTrademark = JSON.parseObject(obj, classOf[BaseTrademark])
      trademark
    })

    //保存hbase
    import org.apache.phoenix.spark._
    tradeMarkDStream.foreachRDD(elem => {
      elem.saveToPhoenix("gmall0421_base_trademark", Seq("ID", "TM_NAME"),
        new Configuration,
        Some("hadoop102,hadoop103,hadoop104:2181"))
      OffsetManagerUtil.saveOffset(topic, group, offsetRanges)
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
