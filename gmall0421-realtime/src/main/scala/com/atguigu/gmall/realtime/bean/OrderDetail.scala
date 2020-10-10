package com.atguigu.gmall.realtime.bean

/**
 * @ClassName: OrderDetail
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/20  19:15
 * @Version: 1.0
 */
case class OrderDetail(id: Long,
                       order_id:Long,
                       sku_id: Long,
                       order_price: Double,
                       sku_num:Long,
                       sku_name: String,
                       create_time: String,

                       //作为维度数据 需要关联的字段
                       var spu_id: Long,
                       var tm_id: Long,
                       var category3_id: Long,
                       var spu_name: String,
                       var tm_name: String,
                       var category3_name: String
                      )