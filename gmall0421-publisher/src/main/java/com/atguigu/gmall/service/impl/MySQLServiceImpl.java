package com.atguigu.gmall.service.impl;

import com.atguigu.gmall.mapper.TrademarkStatMapper;
import com.atguigu.gmall.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @ClassName: MySQLServiceImpl
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/24  18:18
 * @Version: 1.0
 */
@Service
public class MySQLServiceImpl implements MySQLService {

    @Autowired
    TrademarkStatMapper trademarkStatMapper;

    @Override
    public List<Map> getTrademardStat(String startDate, String endDate, int topN) {
        return trademarkStatMapper.selectTradeSum(startDate, endDate, topN);
    }


}