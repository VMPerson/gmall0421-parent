package com.atguigu.gmall.realtime.bean

/**
 * @ClassName: UserStatus
 * @Description: TODO 用户是否消费过
 * @Author: VmPerson
 * @Date: 2020/9/18  14:00
 * @Version: 1.0
 */
case class UserStatus(
                       userId: String, //用户id
                       ifConsumed: String //是否消费过   0首单   1非首单
                     )
