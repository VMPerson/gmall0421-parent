package com.atguigu.gmall.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName: LoggerController
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/8  11:06
 * @Version: 1.0
 */

@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping("/applog")
    public String applog(@RequestBody String jsonLog) {
        System.out.println(jsonLog);
        log.info(jsonLog);

        JSONObject jsonObject = JSON.parseObject(jsonLog);
        if (jsonObject.getJSONObject("start") != null) {
            kafkaTemplate.send("gmall0421_start", jsonLog);
        } else {
            kafkaTemplate.send("gmall0421_action", jsonLog);
        }

        return "success";
    }
}