package com.atguigu.gmall.realtime.dwd

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{OrderInfo, ProviceInfo, UserInfo, UserStatus}
import com.atguigu.gmall.realtime.utils._
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName: OrderInfoApp
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/16  20:29
 * @Version: 1.0
 */
object OrderInfoApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("OrderInfoApp").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    var topic = "ods_order_info"
    var groupId = "ods_order_group"

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
    //做一下映射，我们只取value的值
    val ds: DStream[OrderInfo] = offsetDStream.map(elem => {
      val str: String = elem.value()
      val orderInfo: OrderInfo = JSON.parseObject(str, classOf[OrderInfo])
      val create_time: String = orderInfo.create_time
      val d1: Array[String] = create_time.split(" ")
      orderInfo.create_date = d1(0) //设置创建日期
      orderInfo.create_hour = d1(0).split(":")(0) //设置小时
      orderInfo
    })

    //一分区为单位进行查询hbase，是否是首单
    val setStatusDStream: DStream[OrderInfo] = ds.mapPartitions(orderInfos => {
      val orders: List[OrderInfo] = orderInfos.toList
      val userIds: List[Long] = orders.map(_.user_id)
      var sql = s"select user_id,if_consumed from user_stat0421 where user_id in ('${userIds.mkString(",")}')"
      val result: List[JSONObject] = PhoenixUtil.queryList(sql)
      val list: List[String] = result.map(_.getString("USER_ID"))

      for (orderInfo <- orders) {
        if (list.contains(orderInfo.user_id.toString)) {
          orderInfo.if_first_order = "0"
        } else {
          orderInfo.if_first_order = "1"
        }
      }
      orders.toIterator
    })


    //过滤掉一个周期内，相同用户id单子，第一个为首单，第二个为非首单
    val groupByKeyDStream: DStream[(Long, Iterable[OrderInfo])] =
      setStatusDStream.map(elem => {
        (elem.user_id, elem)
      }).groupByKey()

    val orderInfoWithFirst: DStream[OrderInfo] = groupByKeyDStream.flatMap {
      case (user_id, orderInfos) => {
        if (orderInfos.size > 1) {
          val infoes: List[OrderInfo] = orderInfos.toList.sortWith {
            (order1, order2) => {
              order1.create_time < order2.create_time
            }
          }
          val infoFirst: OrderInfo = infoes(0)
          if (infoFirst.if_first_order == "1") {
            for (i <- 1 until (infoes.size)) {
              infoes(i).if_first_order = "0"
            }
          }
          infoes
        } else {
          orderInfos.toIterator
        }
      }
    }

    //将事实表和地区维度表进行关联（现事实表在DStream里，地区维度表在hbase里）
    //方案一：以分区为单位进行查询填补事实表字段
    /*    val orderInfoDStreamWithProvince: DStream[OrderInfo] = orderInfoWithFirst.mapPartitions(orderInfoItr => {
          val orderInfoList: List[OrderInfo] = orderInfoItr.toList
          val list: List[Long] = orderInfoList.map(_.province_id)
          var provinceSql: String = s"select ID ,NAME, AREA_CODE,ISO_CODE  from GMALL0421_PROVINCE_INFO where ID in '${list.mkString("','")}'"
          val jSONObjects: List[JSONObject] = PhoenixUtil.queryList(provinceSql)
          val provinceInfos: Map[String, ProviceInfo] = jSONObjects.map(obj => {
            val info: ProviceInfo = JSON.parseObject(obj.toString, classOf[ProviceInfo])
            (info.id, info)
          }).toMap
          for (orderInfo <- orderInfoList) {
            val proviceInfo: ProviceInfo = provinceInfos.getOrElse(orderInfo.province_id.toString, null)
            orderInfo.province_area_code = proviceInfo.area_code
            orderInfo.province_iso_code = proviceInfo.iso_code
            orderInfo.province_name = proviceInfo.name
          }
          orderInfoList.toIterator
        }
        )*/

    //第二种方案
    //第一种方式还是过于频繁，以采集周期为单位采集最为合适，缓存也不行，缓存的话，数据更改的话就会有问题,
    // 先把所有的省份查询出来，采用广播变量，使每个executor端都是有一份数据副本，减少数据IO和传输以及序列化反序列化
    val orderInfoDStreamWithProvince: DStream[OrderInfo] = orderInfoWithFirst.transform(ds => {
      var provinceSql: String = s"select ID ,NAME, AREA_CODE,ISO_CODE  from GMALL0421_PROVINCE_INFO"
      val jSONObjects: List[JSONObject] = PhoenixUtil.queryList(provinceSql)
      val provinceMap: Map[String, ProviceInfo] = jSONObjects.map(obj => {
        val info: ProviceInfo = JSON.parseObject(obj.toString, classOf[ProviceInfo])
        (info.id, info)
      }).toMap
      val provinceInfoBC: Broadcast[Map[String, ProviceInfo]] = ssc.sparkContext.broadcast(provinceMap)
      ds.mapPartitions(orderInfoItr => {
        val orderIfoList: List[OrderInfo] = orderInfoItr.toList
        for (orderInfo <- orderIfoList) {
          val provinceInfo: ProviceInfo = provinceInfoBC.value.getOrElse(orderInfo.province_id.toString, null)
          orderInfo.province_area_code = provinceInfo.area_code
          orderInfo.province_iso_code = provinceInfo.iso_code
          orderInfo.province_name = provinceInfo.name
        }
        orderIfoList.toIterator
      })
    })


    //关联用户表 因为用户数据量有点大 ，所以采用广播变量的方式不合适
    val orderInfoDStreamWithProvinceWithUser: DStream[OrderInfo] = orderInfoDStreamWithProvince.mapPartitions(orderInfoItr => {
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      val userIdList: List[Long] = orderInfoList.map(_.user_id)
      var userSql: String = s"select id,user_level,birthday,gender,age_group,gender_name from gmall0421_user_info where id in('${userIdList.mkString("','")}')"
      val jSONObjects: List[JSONObject] = PhoenixUtil.queryList(userSql)
      val userInfoMap: Map[String, UserInfo] = jSONObjects.map(obj => {
        val info: UserInfo = JSON.parseObject(obj.toString, classOf[UserInfo])
        (info.id, info)
      }).toMap

      for (orderInfo <- orderInfoList) {
        val userInfo: UserInfo = userInfoMap.getOrElse(orderInfo.user_id.toString, null)
        orderInfo.user_gender = userInfo.gender_name
        orderInfo.user_age_group = userInfo.age_group
      }
      orderInfoList.toIterator
    })


    //保存用户状态
    import org.apache.phoenix.spark._
    orderInfoDStreamWithProvinceWithUser.foreachRDD(rdd => {
      //优化：对RDD进行缓存
      rdd.cache()
      //先过滤状态为1的
      val orderInfoRDD: RDD[OrderInfo] = rdd.filter(_.if_first_order == "1")
      //往hbase保存对象的时候，需要字段对应 ，所以要转化为用户状态实体类
      val userStatRDD: RDD[UserStatus] = orderInfoRDD.map(orderInfo => {
        UserStatus(orderInfo.user_id.toString, orderInfo.if_first_order)
      })
      userStatRDD.saveToPhoenix("USER_STAT0421", Seq("USER_ID", "IF_CONSUMED"),
        new Configuration,
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
      //将数据保存到ES
      rdd.foreachPartition(elem => {
        val orderInfos: List[(String, OrderInfo)] = elem.toList.map(orderInfo => {
          (orderInfo.id.toString, orderInfo)
        })
        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
        //先把ES注释掉，太消耗性能
       // MyEsUtil.bulkInsert(orderInfos, "gmall0421_order_info_" + dt)
        //将数据推回kafka的下一层主题 ,使用封装的工具类，然后需要使用SerializeConfig 防止反复转换
        for ((orderInfoID, orderInfo) <- orderInfos) {
          MyKafkaSink.send("dwd_order_info", JSON.toJSONString(orderInfo, new SerializeConfig(true)))
        }
      })
      OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()
  }


}
