package com.atguigu.gmall.controller;

import com.atguigu.gmall.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * @ClassName: DataVController
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/24  18:50
 * @Version: 1.0
 */
@RestController
public class DataVController {

    @Autowired
    MySQLService mysqlService;

    @GetMapping("/trademark-sum")
    public Object trademarkSum(@RequestParam("start_date") String startDate,
                               @RequestParam("end_date") String endDate,
                               @RequestParam("topN") int topN) {

        List<Map> trademardSum = mysqlService.getTrademardStat(startDate, endDate, topN);
        return trademardSum;
    }

}