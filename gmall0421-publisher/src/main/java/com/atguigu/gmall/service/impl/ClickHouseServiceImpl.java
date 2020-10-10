package com.atguigu.gmall.service.impl;

import com.atguigu.gmall.mapper.OrderWideMapper;
import com.atguigu.gmall.service.ClickHouseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: ClickHouseServiceImpl
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/23  16:49
 * @Version: 1.0
 */
@Service
public class ClickHouseServiceImpl implements ClickHouseService {

    @Autowired
    OrderWideMapper orderWideMapper;


    /**
     * @描述 获取当日交易总额
     * @参数 [date]
     * @返回值 java.math.BigDecimal
     * @创建人 VmPerson
     * @创建时间 2020/9/23 16:49
     * @Version 1.0
     */
    @Override
    public BigDecimal getOrderAmountTotal(String date) {
        return orderWideMapper.getOrderAmountTotal(date);
    }

    /**
     * @描述 获取当日交易分时额
     * @参数 [date]
     * @返回值 java.util.Map<java.lang.String, java.math.BigDecimal>
     * @创建人 VmPerson
     * @创建时间 2020/9/23 16:50
     * @Version 1.0
     */
    @Override
    public Map<String, BigDecimal> getOrderAmountHour(String date) {
        HashMap<String, BigDecimal> map = new HashMap<>();
        List<Map> orderAmountHourMap = orderWideMapper.getOrderAmountHourMap(date);
        orderAmountHourMap.forEach(elem-> {
                   map.put(String.format("%02d", elem.get("hr")), (BigDecimal)elem.get("sum_amount"));
                });
        return map;
    }
}