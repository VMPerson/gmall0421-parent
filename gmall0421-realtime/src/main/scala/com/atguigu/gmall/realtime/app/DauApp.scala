package com.atguigu.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.DauInfo
import com.atguigu.gmall.realtime.utils.{MyEsUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


/**
 * @ClassName: DauApp
 * @Description: TODO   需求一：获取日活数据，读取kafka数据,将数据通过redis去重,存储到ES
 * @Author: VmPerson
 * @Date: 2020/9/13  16:09
 * @Version: 1.0
 */
object DauApp {

  private val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH mm")

  def main(args: Array[String]): Unit = {

    //设置spark的上下文对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("kafka_to_redis_to_elasticserach")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    var topic_start = "gmall0421_start"
    var groupId = "gmall_log"

    /**
     * 需求一：步骤1
     * 从kafka里面读取数据（启动日志）
     */

    //优化： 手动维护offset
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null

    val mapPTL: Map[TopicPartition, Long] = OffsetManagerUtil.getOffSet(topic_start, groupId)
    if (mapPTL != null && mapPTL.size > 0) {
      kafkaDStream = MyKafkaUtil.collectKafkaStream(topic_start, ssc, groupId, mapPTL)
    } else {
      kafkaDStream = MyKafkaUtil.collectKafkaStream(topic_start, ssc, groupId)
    }

    //从读取的kafka数据中读取偏移量
    //创建一个空数组，用于封装offset信息
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    val offsetDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform {
      elem => {
        offsetRanges = (elem.asInstanceOf[HasOffsetRanges]).offsetRanges
        elem
      }
    }

    val jsonDStream: DStream[JSONObject] = offsetDStream.map(elem => {
      val jsonValue: String = elem.value()
      val jObject: JSONObject = JSON.parseObject(jsonValue)
      val longTime: lang.Long = jObject.getLong("ts")
      val date: String = simpleDateFormat.format(new Date(longTime))
      val yhm: Array[String] = date.split(" ")
      jObject.put("dt", yhm(0))
      jObject.put("hr", yhm(1))
      jObject.put("mi", yhm(2))
      jObject
    })


    /**
     * 需求一：步骤2
     * 将从kafka里面读取的数据，通过redis进行去重
     * （此时，可以一条一条去获取DStream,但是这样频繁获取redis连接的方式也不合适，我们可以采取通过分区为单位进行获取）
     */
    val filterDStream: DStream[JSONObject] = jsonDStream.mapPartitions {
      elem => {
        //获取jedis
        val jedis: Jedis = MyRedisUtil.getClient()
        //新建集合存储对象
        val listBuffer: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
        for (jsonObject <- elem) {
          val dt: String = jsonObject.getString("dt")
          val mid: String = jsonObject.getJSONObject("common").getString("mid")
          //加个前缀，使用： 显示层级关系
          var daukey = "dau:" + dt
          val response: Long = jedis.sadd(daukey, mid)
          if (response == 1L) listBuffer.append(jsonObject)
        }
        jedis.close()
        listBuffer.toIterator
      }
    }

    /**
     * 需求一：步骤3
     * 通过Redis去重后，将数据存储到ES
     * 优化： 利用ES再次进行去重
     */
    //首先将dStream遍历，遍历之后将每个ds进行分区为单位进行遍历，就是获取到的每个分区的rdd,对每个分区每个rdd进行转换
    filterDStream.foreachRDD { ds => {
      ds.foreachPartition { partitonRdd => {
        val dauList: List[(String, DauInfo)] = partitonRdd.map {
          rdd => {
            val commonObj: JSONObject = rdd.getJSONObject("common")
            val info: DauInfo = DauInfo(
              commonObj.getString("mid"),
              commonObj.getString("uid"),
              commonObj.getString("ar"),
              commonObj.getString("ch"),
              commonObj.getString("vc"),
              rdd.getString("dt"),
              rdd.getString("hr"),
              rdd.getString("mi"),
              rdd.getLong("ts")
            )

            (commonObj.getString("mid"), info)
          }
        }.toList
        //格式化日期，索引要用
        var date: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        MyEsUtil.bulkInsert(dauList, "gmall0421_dau_info_" + date)
      }
      }
      OffsetManagerUtil.saveOffset(topic_start, groupId, offsetRanges)
    }

    }

    ssc.start()
    ssc.awaitTermination()

  }


}
