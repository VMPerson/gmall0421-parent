package com.atguigu.gmall.service.impl;

import com.atguigu.gmall.service.ESService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: ESServiceImpl
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/15  17:50
 * @Version: 1.0
 */
@Service
public class ESServiceImpl implements ESService {

    @Autowired
    private JestClient jestClient;

    @Override
    public Long getDauTotal(String dt) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchSourceBuilder query = searchSourceBuilder.query(new MatchAllQueryBuilder());
        String indexName = "gmall0421_dau_info_" + dt + "-query";

        Search search = new Search.Builder(query.toString()).addIndex(indexName).addType("_doc").build();

        try {
            SearchResult execute = jestClient.execute(search);
            return execute != null ? execute.getTotal() : 0L;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询异常！");
        }
    }


    /**
     * @描述 获取小时活跃设备
     * @参数 [dt]
     * @返回值 java.util.Map<java.lang.String, java.lang.String>
     * @创建人 VmPerson
     * @创建时间 2020/9/15 18:33
     * @Version 1.0
     */
    @Override
    public Map<String, String> getDauHour(String dt) {

        HashMap<String, String> map = new HashMap<>();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(AggregationBuilders.terms("groupby_hour").field("hr").size(24));

        String indexName = "gmall0421_dau_info_" + dt + "-query";
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();
        try {
            SearchResult result = jestClient.execute(search);
            if (result.getAggregations().getTermsAggregation("groupby_hour") != null) {
                List<TermsAggregation.Entry> groupby_hour = result.getAggregations().getTermsAggregation("groupby_hour").getBuckets();
                for (TermsAggregation.Entry entry : groupby_hour) {
                    map.put(entry.getKey(), entry.getCount().toString());
                }
            }

            return map;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES 查询异常");
        }

    }


}