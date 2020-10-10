package com.atguigu.gmall.service;

import java.math.BigDecimal;
import java.util.Map;

/**
 * @ClassName: ClickHouseService
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/23  16:46
 * @Version: 1.0
 */
public interface ClickHouseService {

    //获取当日交易总额
    public BigDecimal getOrderAmountTotal(String date);

    //获取当日分时交易额
    public Map<String, BigDecimal> getOrderAmountHour(String date);


}