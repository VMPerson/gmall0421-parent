package com.atguigu.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.utils.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName: ODSMaxWell
 * @Description: TODO 先MAxWell将数据同步到kafka，
 *               此类进行数据分流处理，针对不同的表名，将数据封装到kafka不同的主题里面去
 * @Author: VmPerson
 * @Date: 2020/9/16  11:45
 * @Version: 1.0
 */
object ODSMaxWell {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ODSMaxWell")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    //这个主题名称是maxwell配置文件里面配置的
    var topic = "gmall0421_db_m"
    var groupId = "db_maxwell"
    //从redis读取该主题该消费者组的偏移量
    val partitionToLong: Map[TopicPartition, Long] = OffsetManagerUtil.getOffSet(topic, groupId)

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (partitionToLong != null && partitionToLong.size > 0) {
      recordDStream = MyKafkaUtil.collectKafkaStream(topic, ssc, groupId, partitionToLong)
    } else {
      recordDStream = MyKafkaUtil.collectKafkaStream(topic, ssc, groupId)
    }
    //记录偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val ds: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      elem => {
        offsetRanges = elem.asInstanceOf[HasOffsetRanges].offsetRanges
        elem
      }
    }
    //做映射，只取value部分转化为json对象
    val tmp: DStream[JSONObject] = ds.map(elem => {
      val str: String = elem.value()
      val jsonOnj: JSONObject = JSON.parseObject(str)
      jsonOnj
    })

    //遍历DS，解析数据，根据不同的表名，将数据封装到不同的kafka主题里面去
    tmp.foreachRDD(elem => {
      elem.foreach(con => {
        val tableName: String = con.getString("table")
        val opType: String = con.getString("type")
        val nObject: JSONObject = con.getJSONObject("data")
        if (nObject != null && !nObject.isEmpty) {
          if (
            (tableName.equals("order_info") && "insert".equals(opType))
              || (tableName.equals("order_detail") && "insert".equals(opType))
              || tableName.equals("base_province")
              || tableName.equals("user_info")
              || tableName.equals("sku_info")
              || tableName.equals("base_trademark")
              || tableName.equals("base_category3")
              || tableName.equals("spu_info")
          ) {
            var tn: String = "ods_" + tableName
            MyKafkaSink.send(tn, nObject.toString())
          }
        }
      })
      //保存偏移量
      OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()
  }


}
