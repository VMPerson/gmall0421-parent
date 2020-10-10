package com.atguigu.gmall.realtime.bean

/**
 * @ClassName: UserInfo
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/18  20:26
 * @Version: 1.0
 */
case class UserInfo( id:String,
                     user_level:String,
                     birthday:String,
                     gender:String,
                     var age_group:String,//年龄段
                     var gender_name:String //性别
                   )
