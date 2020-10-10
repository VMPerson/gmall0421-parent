package com.atguigu.gmall.realtime.bean

/**
 * @ClassName: UserStatus
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/18  14:00
 * @Version: 1.0
 */
case class UserStatus(
                       userId:String,  //用户id
                       ifConsumed:String //是否消费过   0首单   1非首单
                     )
