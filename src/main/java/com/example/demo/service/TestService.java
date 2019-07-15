package com.example.demo.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @Author mengxiangzhi
 * @CreatTime 2019/7/15
 * <p>
 * 自动消费
 **/
@Component
public class TestService {

    @KafkaListener(topics = "test123", groupId = "test")
    public void receive(String message) {
        System.out.println("消费消息:" + message);
    }


}
