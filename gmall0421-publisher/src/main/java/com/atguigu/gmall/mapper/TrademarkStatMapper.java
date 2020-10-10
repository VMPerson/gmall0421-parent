package com.atguigu.gmall.mapper;

import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

/**
 * @ClassName: TrademarkStatMapper
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/24  18:15
 * @Version: 1.0
 */
public interface TrademarkStatMapper {

    public List<Map> selectTradeSum(@Param("start_date") String startDate ,
                                    @Param("end_date")String endDate,
                                    @Param("topN")int topN);

}