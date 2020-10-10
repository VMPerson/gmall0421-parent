package com.atguigu.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.SpuInfo
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName: SpuInfoApp
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/20  19:48
 * @Version: 1.0
 */
object SpuInfoApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SpuInfoApp")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_spu_info";
    val groupId = "dim_spu_info_group"

    val offset: Map[TopicPartition, Long] = OffsetManagerUtil.getOffSet(topic, groupId)

    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null

    if (offset != null && offset.size > 0) {
      inputDstream = MyKafkaUtil.collectKafkaStream(topic, ssc, groupId, offset)
    } else {
      inputDstream = MyKafkaUtil.collectKafkaStream(topic, ssc, groupId)
    }

    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    val objectDstream: DStream[SpuInfo] = inputGetOffsetDstream.map {
      record => {
        val jsonStr: String = record.value()
        val obj: SpuInfo = JSON.parseObject(jsonStr, classOf[SpuInfo])
        obj
      }
    }

    import org.apache.phoenix.spark._
    objectDstream.foreachRDD { rdd =>
      rdd.saveToPhoenix("GMALL0421_SPU_INFO", Seq("ID", "SPU_NAME")
        , new Configuration, Some("hadoop102,hadoop103,hadoop104:2181"))
      OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()

  }


}
