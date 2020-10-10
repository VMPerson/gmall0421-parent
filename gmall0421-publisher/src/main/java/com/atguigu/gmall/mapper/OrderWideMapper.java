package com.atguigu.gmall.mapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: OrderWideMapper
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/23  14:00
 * @Version: 1.0
 */

public interface OrderWideMapper {


    //当日交易总额
    public BigDecimal getOrderAmountTotal(String date);

    //查询当日交易额分时明细
    public List<Map> getOrderAmountHourMap(String data);


}