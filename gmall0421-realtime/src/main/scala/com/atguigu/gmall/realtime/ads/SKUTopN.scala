package com.atguigu.gmall.realtime.ads

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.OrderWide
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerMySql}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.DB
import scalikejdbc.config.DBs

/**
 * @ClassName: SKUTopN
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/24  13:54
 * @Version: 1.0
 */
object SKUTopN {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SKUTopN")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "ads_sku_topN_group"
    val topic = "DWS_ORDER_WIDE";

    //从mysql读取偏移量
    var record: InputDStream[ConsumerRecord[String, String]] = null;
    val partitionToLong: Map[TopicPartition, Long] = OffsetManagerMySql.getOffset(topic, groupId)
    if (partitionToLong != null && partitionToLong.size > 0) {
      record = MyKafkaUtil.collectKafkaStream(topic, ssc, groupId, partitionToLong)
    } else {
      record = MyKafkaUtil.collectKafkaStream(topic, ssc, groupId)
    }
    //记录这批数据的偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val recordDStream: DStream[ConsumerRecord[String, String]] = record.transform(elem => {
      offsetRanges = elem.asInstanceOf[HasOffsetRanges].offsetRanges
      elem
    })


    //对数据及逆行转换 需求：热门商品统计
    val skuDStream: DStream[(String, Int)] = recordDStream.map(elem => {
      val objStr: String = elem.value()
      val orderWide: OrderWide = JSON.parseObject(objStr, classOf[OrderWide])
      (orderWide.sku_id + "_" + orderWide.spu_name, 1)
    })


    val sumDStream: DStream[(String, Int)] = skuDStream.reduceByKey(_ + _)
    //我们需要在clixkhosse里面建立表，存储相应的商品信息

    sumDStream.foreachRDD(elem => {
      val array: Array[(String, Int)] = elem.collect()
      if (array != null && array.size > 0) {
        DBs.setup()
        DB.localTx({
          implicit session => {

          }
        })


      }


    })


  }


}
