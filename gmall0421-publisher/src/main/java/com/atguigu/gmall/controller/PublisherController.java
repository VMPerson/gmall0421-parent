package com.atguigu.gmall.controller;

import com.atguigu.gmall.service.ClickHouseService;
import com.atguigu.gmall.service.ESService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: ESController
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/15  17:51
 * @Version: 1.0
 */
@RestController
public class PublisherController {

    @Autowired
    private ESService esService;
    @Autowired
    private ClickHouseService clickHouseService;


    /**
     * @描述 日统计
     * @参数 http://publisher:8070/realtime-total?date=2019-02-01
     * @返回值 java.lang.Object
     * @创建人 VmPerson
     * @创建时间 2020/9/23 18:14
     * @Version 1.0
     */
    @RequestMapping("/realtime-total")
    public Object realTimeTotal(@RequestParam("date") String dt) {
        Long dayTotal = esService.getDauTotal(dt);
        ArrayList<Map<String, String>> list = new ArrayList<>();
        HashMap<String, String> map = new HashMap<>();
        map.put("id", "dau");
        map.put("name", "新增日活");
        map.put("value", dayTotal.toString());
        list.add(map);

        HashMap<String, String> map2 = new HashMap<>();
        map2.put("id", "new_mid");
        map2.put("name", "新增设备");
        map2.put("value", "233");
        list.add(map2);

        HashMap<String, String> map3 = new HashMap<>();
        map3.put("id", "order_amount");
        map3.put("name", "新增交易额");
        map3.put("value", clickHouseService.getOrderAmountTotal(dt).toString());
        list.add(map3);

        return list;
    }

    /**
     * @描述 日小时统计
     * @参数 [id, dt] http://publisher:8070/realtime-hour?id=dau&date=2019-02-01
     * @返回值 java.lang.Object
     * @创建人 VmPerson
     * @创建时间 2020/9/23 18:15
     * @Version 1.0
     */
    @RequestMapping("/realtime-hour")
    public Object realTimeDauHour(@RequestParam(value = "id", defaultValue = "-1") String id, @RequestParam("date") String dt) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date dat = format.parse(dt);
        Date yDay = DateUtils.addDays(dat, -1);
        String yesterday = format.format(yDay);
        if (id.equals("dau")) {
            Map<String, Map<String, String>> map = new HashMap<>();
            Map<String, String> todayResult = esService.getDauHour(dt);
            Map<String, String> yesterdayResult = esService.getDauHour(yesterday);
            map.put("yesterday", yesterdayResult);
            map.put("today", todayResult);
            return map;
        } else if (id.equals("order_amount")) {
            Map<String, BigDecimal> todayMap = clickHouseService.getOrderAmountHour(dt);
            Map<String, BigDecimal> yesterdayMap = clickHouseService.getOrderAmountHour(yesterday);
            HashMap<String, Map<String, BigDecimal>> map = new HashMap<>();
            map.put("yesterday", yesterdayMap);
            map.put("today", todayMap);
            return map;
        } else {
            return null;
        }

    }


}