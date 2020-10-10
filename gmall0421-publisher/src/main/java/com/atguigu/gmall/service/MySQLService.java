package com.atguigu.gmall.service;

import java.util.List;
import java.util.Map;

/**
 * @ClassName: MySQLService
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/24  18:17
 * @Version: 1.0
 */
public interface MySQLService {

    public List<Map> getTrademardStat(String startDate, String endDate, int topN);
}