package com.atguigu.gmall.service;

import java.util.Map;

/**
 * @ClassName: ESService
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/15  17:50
 * @Version: 1.0
 */
public interface ESService {

    /**

     *@描述 获取日新增总数

     *@参数   [dt]

     *@返回值 java.lang.Long

     *@创建人 VmPerson

     *@创建时间 2020/9/15 17:58

     *@Version 1.0

     */
   public Long  getDauTotal(String dt);

    /**

     *@描述 获取小时数据

     *@参数   [dt]

     *@返回值 java.util.Map<java.lang.String,java.lang.String>

     *@创建人 VmPerson

     *@创建时间 2020/9/15 18:33

     *@Version 1.0

     */
    Map<String, String> getDauHour(String dt);
}