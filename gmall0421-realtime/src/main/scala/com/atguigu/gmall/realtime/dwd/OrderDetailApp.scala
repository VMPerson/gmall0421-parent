package com.atguigu.gmall.realtime.dwd

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{OrderDetail, SkuInfo}
import com.atguigu.gmall.realtime.utils.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName: OrderDetailApp
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/20  20:08
 * @Version: 1.0
 */
object OrderDetailApp {

  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setAppName("OrderDetailApp").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    var topic = "ods_order_detail"
    var groupId = "ods_detail_group"

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    //从redis里面获取偏移量
    val partitionToLong: Map[TopicPartition, Long] = OffsetManagerUtil.getOffSet(topic, groupId)
    if (partitionToLong != null && partitionToLong.size > 1) {
      recordDStream = MyKafkaUtil.collectKafkaStream(topic, ssc, groupId, partitionToLong)
    } else {
      recordDStream = MyKafkaUtil.collectKafkaStream(topic, ssc, groupId)
    }

    //记录本次消费偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform(elem => {
      offsetRanges = elem.asInstanceOf[HasOffsetRanges].offsetRanges
      elem
    })


    //对数据进行结构转化
    val ds: DStream[OrderDetail] = offsetDStream.map(elem => {
      val obj: String = elem.value()
      val orderDetail: OrderDetail = JSON.parseObject(obj, classOf[OrderDetail])
      orderDetail
    })

    //从hbase里面获取sku信息与之进行关联
    val orderDetailWithSku: DStream[OrderDetail] = ds.mapPartitions(elem => {
      val orderDetaillist: List[OrderDetail] = elem.toList
      val skuIdList: List[Long] = orderDetaillist.map(_.sku_id)
      //读取hbase sku信息数据
      var sql = s"select id ,tm_id,spu_id,category3_id,tm_name ,spu_name,category3_name  from gmall0421_sku_info  where id in ('${skuIdList.mkString("','")}')"
      val skuList: List[JSONObject] = PhoenixUtil.queryList(sql)
      val skuMap: Map[String, SkuInfo] = skuList.map(sku => {
        val skuInfo: SkuInfo = JSON.parseObject(sku.toString, classOf[SkuInfo])
        (skuInfo.id, skuInfo)
      }).toMap

      for (od <- orderDetaillist) {
        val skuInfo: SkuInfo = skuMap.getOrElse(od.sku_id.toString, null)
        if (skuInfo != null) {
          od.spu_id = skuInfo.spu_id.toLong
          od.spu_name = skuInfo.spu_name.toString
          od.tm_id = skuInfo.tm_id.toLong
          od.tm_name = skuInfo.tm_name
          od.category3_id = skuInfo.category3_id.toLong
          od.category3_name = skuInfo.category3_name
        }
      }
      orderDetaillist.toIterator
    })



    //将数据写会到kafka
    orderDetailWithSku.foreachRDD(rdd => {
      rdd.foreach(od => {
        MyKafkaSink.send("dwd_order_detail", JSON.toJSONString(od, new SerializeConfig(true)))
      })
      OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()

  }


}
