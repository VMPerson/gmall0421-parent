package com.atguigu.gmall.realtime.dws

import java.lang
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall.realtime.utils.{MyKafkaSink, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * @ClassName: OrderWideApp
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/20  20:44
 * @Version: 1.0
 */
object OrderWideApp {

  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setAppName("OrderWideApp").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    /*=========================================从kafka里面读取订单信息==================================================*/
    var orderTopic = "dwd_order_info"
    var orderGroup = "dws_info_group"

    var orderInfoDStream: InputDStream[ConsumerRecord[String, String]] = null
    //从redis里面获取订单详情偏移量
    val orderInfoOffset: Map[TopicPartition, Long] = OffsetManagerUtil.getOffSet(orderTopic, orderGroup)
    if (orderInfoOffset != null && orderInfoOffset.size > 1) {
      orderInfoDStream = MyKafkaUtil.collectKafkaStream(orderTopic, ssc, orderGroup, orderInfoOffset)
    } else {
      orderInfoDStream = MyKafkaUtil.collectKafkaStream(orderTopic, ssc, orderGroup)
    }

    //记录本次消费偏移量
    var orderInfoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoDS: DStream[ConsumerRecord[String, String]] = orderInfoDStream.transform(elem => {
      orderInfoOffsetRanges = elem.asInstanceOf[HasOffsetRanges].offsetRanges
      elem
    })

    //转化为对象流
    val orderDS: DStream[OrderInfo] = orderInfoDS.map(elem => {
      val obj: String = elem.value()
      val orderInfo: OrderInfo = JSON.parseObject(obj, classOf[OrderInfo])
      orderInfo
    })


    /*=========================================从kafka里面读取订单明细信息==================================================*/
    var orderDetailTopic = "dwd_order_detail"
    var orderDetailgroup = "dws_detail_group"

    var orderDetailDStream: InputDStream[ConsumerRecord[String, String]] = null
    //从redis里面获取订单详情偏移量
    val orderDetailOffset: Map[TopicPartition, Long] = OffsetManagerUtil.getOffSet(orderDetailTopic, orderDetailgroup)
    if (orderDetailOffset != null && orderDetailOffset.size > 1) {
      orderDetailDStream = MyKafkaUtil.collectKafkaStream(orderDetailTopic, ssc, orderDetailgroup, orderDetailOffset)
    } else {
      orderDetailDStream = MyKafkaUtil.collectKafkaStream(orderDetailTopic, ssc, orderDetailgroup)
    }

    //记录本次消费偏移量
    var orderdetailOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderDetailDS: DStream[ConsumerRecord[String, String]] = orderDetailDStream.transform(elem => {
      orderdetailOffsetRanges = elem.asInstanceOf[HasOffsetRanges].offsetRanges
      elem
    })

    //转化为实体类
    val orderDetailDs: DStream[OrderDetail] = orderDetailDS.map {
      record => {
        val obj: String = record.value()
        //订单处理  转换成更方便操作的专用样例类
        val orderDetail: OrderDetail = JSON.parseObject(obj, classOf[OrderDetail])
        orderDetail
      }
    }


    /**
     * 两个流  orderDS 和  orderDetailDs
     * 现在进行关联，直接join是有问题的的，问题时可不一个周期内不能把所有的订单下相关联的订单详情获取过来，这样的话数据就会有问题，所以不能直接进行join
     * 现有两种方案：
     * 方案一：通过redis存储 ，用redis存储的话又会存在三种情况 ，针对不同的情况我们都要做处理 （some,some） (some,none) (none,some)
     * 方案二：通过streaming的窗口函数实现， 窗口函数又分为两种：滚动和滑动  采用滚动的方式是不能够解决问题的，我们采用滑动方式解决
     * 通过滑动方式会存在数据重复的情况 ，为了解决数据重复 ，我们通过 redis来解决，通过set类型的来存储（orderid,orderDetailId）
     */


    val ods: DStream[(Long, OrderInfo)] = orderDS.mapPartitions(orderInfoItr => {
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      orderInfoList.map(orderInfo => {
        (orderInfo.id, orderInfo)
      }).toIterator
    })
    val odWin: DStream[(Long, OrderInfo)] = ods.window(Seconds(20), Seconds(5))
    val odds: DStream[(Long, OrderDetail)] = orderDetailDs.mapPartitions(orderDetailItr => {
      val orderDetailList: List[OrderDetail] = orderDetailItr.toList
      orderDetailList.map(orderDetail => {
        (orderDetail.order_id, orderDetail)
      }).toIterator
    })
    val oddsWin: DStream[(Long, OrderDetail)] = odds.window(Seconds(20), Seconds(5))
    val joinDS: DStream[(Long, (OrderInfo, OrderDetail))] = odWin.join(oddsWin)

    val joinRes: DStream[OrderWide] = joinDS.mapPartitions(orderWideItr => {
      //获取jedis
      val jedis: Jedis = MyRedisUtil.getClient()
      val orderWides: ListBuffer[OrderWide] = new ListBuffer[OrderWide]
      for ((orderId, (orderInfo, orderDetail)) <- orderWideItr) {
        val key = "order_join:" + orderId
        val res: lang.Long = jedis.sadd(key, orderDetail.id.toString)
        jedis.expire(key, 600)
        if (res != null && res == 1L) {
          orderWides.append(new OrderWide(orderInfo, orderDetail))
        }
      }
      //关闭资源
      jedis.close()
      orderWides.toIterator
    })

    //金额分摊  金额分摊的话我们要考虑该订单详情是否是所属订单里的最后一单

    //order_origin_sum:[order_id]  	订单的已经计算完的明细的【数量*单价】的合计         为了计算判断是否是最后一单使用
    //order_split_sum: [order_id]	其他明细已经计算好的【实付分摊金额】的合计             当为最后一单时，算实际分摊金额使用

    val joinWithSplitAmountDStream: DStream[OrderWide] = joinRes.mapPartitions(orderWideItr => {
      //获取jedis
      val jedis: Jedis = MyRedisUtil.getClient()

      val orderWideList: List[OrderWide] = orderWideItr.toList
      for (orderWide <- orderWideList) {
        var orderOriginSumkey: String = "order_origin_sum:" + orderWide.order_id
        var orderOriginSum: Double = 0D
        var otherOriginAmount: String = jedis.get(orderOriginSumkey)
        if (otherOriginAmount != null && otherOriginAmount.length > 0) {
          orderOriginSum = otherOriginAmount.toDouble
        }

        var orderSplitSumkey: String = "order_split_sum:" + orderWide.order_id
        var otherSplitSum: Double = 0D
        var otherSplitSumAmount: String = jedis.get(orderSplitSumkey)
        if (otherSplitSumAmount != null && otherSplitSumAmount.length > 0) {
          otherSplitSum = otherSplitSumAmount.toDouble
        }

        var detailOriginAmount: Double = orderWide.sku_price * orderWide.sku_num
        if (detailOriginAmount == (orderWide.original_total_amount - orderOriginSum)) {
          orderWide.final_detail_amount = Math.round((orderWide.final_total_amount - otherSplitSum) * 100D) / 100D
        } else {
          orderWide.final_detail_amount = Math.round((orderWide.final_total_amount * detailOriginAmount / orderWide.original_total_amount) * 100D) / 100D
        }

        //当设置完分摊金额之后，将数据保存到redis
        jedis.setex(orderOriginSumkey, 600, (orderOriginSum + detailOriginAmount).toString)
        jedis.setex(orderSplitSumkey, 600, (otherSplitSum + orderWide.final_detail_amount).toString)
      }
      //关闭资源
      jedis.close()
      orderWideList.toIterator
    })


    //将数据保存到clickhouse
    val sparkSession: SparkSession = SparkSession.builder().appName("clickhouse_sparksession").getOrCreate()
    import sparkSession.implicits._

    joinWithSplitAmountDStream.foreachRDD(rdd => {
      rdd.cache()
      //机器性能原因，暂时先不clickHour保存
      val df: DataFrame = rdd.toDF()
      df.write.mode(SaveMode.Append)
        .option("batchsize", "100")
        .option("isolationLevel", "NONE") // 设置事务
        .option("numPartitions", "4") // 设置并发
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
        .jdbc("jdbc:clickhouse://hadoop102:8123/default", "t_order_wide_0421", new Properties())

      //将数据保存到kafka供下层使用
      rdd.foreach(ow => {
        MyKafkaSink.send("DWS_ORDER_WIDE", JSON.toJSONString(ow, new SerializeConfig(true)))
      })

      //提交偏移量
      OffsetManagerUtil.saveOffset(orderTopic, orderGroup, orderInfoOffsetRanges)
      OffsetManagerUtil.saveOffset(orderDetailTopic, orderDetailgroup, orderdetailOffsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()

  }


}
