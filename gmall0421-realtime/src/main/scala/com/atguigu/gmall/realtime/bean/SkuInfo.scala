package com.atguigu.gmall.realtime.bean

/**
 * @ClassName: SkuInfo
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/20  19:24
 * @Version: 1.0
 */
case class SkuInfo(id:String ,
                   spu_id:String ,
                   price:String ,
                   sku_name:String ,
                   tm_id:String ,
                   category3_id:String ,
                   create_time:String,

                   var category3_name:String,
                   var spu_name:String,
                   var tm_name:String
                  )
