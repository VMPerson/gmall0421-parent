package com.atguigu.gmall.realtime.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.SkuInfo
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
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
 * @ClassName: SkuInfoApp
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/20  19:51
 * @Version: 1.0
 */
object SkuInfoApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SkuInfoApp")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_sku_info";
    val groupId = "dim_sku_info_group"

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

    val objectDstream: DStream[SkuInfo] = inputGetOffsetDstream.map {
      record => {
        val jsonStr: String = record.value()
        val obj: SkuInfo = JSON.parseObject(jsonStr, classOf[SkuInfo])
        obj
      }
    }

    //维度退化，将sku和 品牌、分类、spu进行关联在这里进行
    val ds: DStream[SkuInfo] = objectDstream.transform(rdd => {
      if (rdd != null) {
        //tm_name
        val tmSql = "select id ,tm_name  from gmall0421_base_trademark"
        val tmList: List[JSONObject] = PhoenixUtil.queryList(tmSql)
        val tmMap: Map[String, JSONObject] = tmList.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap

        //category3
        val category3Sql = "select id ,name from gmall0421_base_category3"
        val category3List: List[JSONObject] = PhoenixUtil.queryList(category3Sql)
        val category3Map: Map[String, JSONObject] = category3List.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap

        // spu
        val spuSql = "select id ,spu_name  from gmall0421_spu_info"
        val spuList: List[JSONObject] = PhoenixUtil.queryList(spuSql)
        val spuMap: Map[String, JSONObject] = spuList.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap

        //汇总到一个list,进行广播
        val maps: List[Map[String, JSONObject]] = List[Map[String, JSONObject]](category3Map, tmMap,spuMap)
        val threeMapBC: Broadcast[List[Map[String, JSONObject]]] = ssc.sparkContext.broadcast(maps)

        val skuInfoRDD: RDD[SkuInfo] = rdd.mapPartitions {
          skuInfoItr => {
            //ex
            val dimList: List[Map[String, JSONObject]] = threeMapBC.value
            val category3Map: Map[String, JSONObject] = dimList(0)
            val tmMap: Map[String, JSONObject] = dimList(1)
            val spuMap: Map[String, JSONObject] = dimList(2)

            val skuInfoList: List[SkuInfo] = skuInfoItr.toList
            for (skuInfo <- skuInfoList) {
              val category3JsonObj: JSONObject = category3Map.getOrElse(skuInfo.category3_id, null)
              if (category3JsonObj != null) {
                skuInfo.category3_name = category3JsonObj.getString("NAME")
              }
              val tmJsonObj: JSONObject = tmMap.getOrElse(skuInfo.tm_id, null)
              if (tmJsonObj != null) {
                skuInfo.tm_name = tmJsonObj.getString("TM_NAME")
              }
              val spuJsonObj: JSONObject = spuMap.getOrElse(skuInfo.spu_id, null)
              if (spuJsonObj != null) {
                skuInfo.spu_name = spuJsonObj.getString("SPU_NAME")
              }
            }
            skuInfoList.toIterator
          }
        }
        skuInfoRDD
      } else {
        rdd
      }
    })

    import org.apache.phoenix.spark._
    ds.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL0421_SKU_INFO",
        Seq("ID", "SPU_ID", "PRICE", "SKU_NAME", "TM_ID", "CATEGORY3_ID", "CREATE_TIME", "CATEGORY3_NAME", "SPU_NAME", "TM_NAME"),
        new Configuration,
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
      OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()
  }


}
