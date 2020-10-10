package com.atguigu.gmall.realtime.ads

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.OrderWide
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerMySql}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

import scala.collection.mutable.ListBuffer

/**
 * @ClassName: TrademarkStatApp 品牌分析
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/23  20:24
 * @Version: 1.0
 */
object TrademarkStatApp {


  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setAppName("TrademarkStatApp").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "ads_trademark_stat_group"
    val topic = "DWS_ORDER_WIDE";

    //从mysql里面获取偏移量
    var record: InputDStream[ConsumerRecord[String, String]] = null
    val partitionToLong: Map[TopicPartition, Long] = OffsetManagerMySql.getOffset(topic, groupId)
    if (partitionToLong != null && partitionToLong.size > 0) {
      record = MyKafkaUtil.collectKafkaStream(topic, ssc, groupId, partitionToLong)
    } else {
      record = MyKafkaUtil.collectKafkaStream(topic, ssc, groupId)
    }

    //获取本批次偏移量结束点
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val recordDs: DStream[ConsumerRecord[String, String]] = record.transform(elem => {
      offsetRanges = elem.asInstanceOf[HasOffsetRanges].offsetRanges
      elem
    })

    //做转换
    val trademarkAmountDstream: DStream[(String, Double)] = recordDs.map(elem => {
      val value: String = elem.value()
      val orderWide: OrderWide = JSON.parseObject(value, classOf[OrderWide])
      (orderWide.tm_id + "_" + orderWide.tm_name, orderWide.final_detail_amount)
    })
    val res: DStream[(String, Double)] = trademarkAmountDstream.reduceByKey(_ + _)


    //往mysql里面插入数据 ,涉及到分布式事务的问题，我们foreachRDD里先collect收集到driver端来执行
    /*  res.foreachRDD(elem => {
        val array: Array[(String, Double)] = elem.collect()
        if (array != null && array.size > 0) {
          DBs.setup()
          DB.localTx(
            {
              implicit session => {
                val formator = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                for ((tm, amount) <- array) {
                  val statTime: String = formator.format(new Date())
                  val tmStr: Array[String] = tm.split("_")
                  val tm_id: String = tmStr(0)
                  val tm_name: String = tmStr(1)
                  val resultAmount: Double = Math.round(amount * 100D) / 100D

                  SQL("insert into trademark_amount_stat(stat_time,trademark_id,trademark_name,amount) values(?,?,?,?)")
                    .bind(statTime, tm_id, tm_name, resultAmount)
                    .update().apply()
                }

                //写入偏移量
                for (offsetRange <- offsetRanges) {
                  val partition: Int = offsetRange.partition
                  val untilOffset: Long = offsetRange.untilOffset
                  SQL("replace into offset_0421 values(?,?,?,?)")
                    .bind(topic, groupId, partition, untilOffset)
                    .update()
                    .apply()
                }
              }
            })
        }
      })
  */

    //通过批量的方式往数据插入数据
    res.foreachRDD(elem => {
      val array: Array[(String, Double)] = elem.collect()

      if (array != null && array.size > 0) {
        DBs.setup()
        DB.localTx({
          implicit session => {
            val formate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val dataTime: String = formate.format(new Date())
            val listBuffer: ListBuffer[Seq[Any]] = new ListBuffer[Seq[Any]]
            for ((tm, amount) <- array) {
              val tmStr: Array[String] = tm.split("_")
              val tm_id: String = tmStr(0)
              val tm_name: String = tmStr(1)
              val resAmount: Double = Math.round(amount * 100D) / 100D
              listBuffer.append(Seq(dataTime, tm_id, tm_name, resAmount))
            }
            SQL("insert into trademark_amount_stat(stat_time,trademark_id,trademark_name,amount) values(?,?,?,?)")
              .batch(listBuffer.toSeq: _*).apply()

            //写入偏移量
            for (offsetRange <- offsetRanges) {
              val partition: Int = offsetRange.partition
              val untilOffset: Long = offsetRange.untilOffset
              SQL("replace into offset_0421 values(?,?,?,?)")
                .bind(topic, groupId, partition, untilOffset)
                .update()
                .apply()
            }
          }
        })
      }
    })
    ssc.start()
    ssc.awaitTermination()

  }


}
