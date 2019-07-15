package com.example.demo.controller;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.Arrays;

/**
 * @Author mengxiangzhi
 * @CreatTime 2019/7/10
 **/
@RestController
@RequestMapping("/kafka")
public class TestController {
    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    private ConsumerFactory consumerFactory;
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;
    @Value("${spring.kafka.template.default-topic}")
    private String topics;

    /**
     * 生产者
     */
    @RequestMapping("/test")
    public String test(@RequestParam(value = "test") String test) {
        ListenableFuture listenableFuture = kafkaTemplate.send(topics, test);
        listenableFuture.addCallback(success -> System.out.println("成功"), fail -> System.out.println("失败"));
        return "success";
    }

    /**
     * 手动消费者
     */
    @RequestMapping("/test1")
    public void receive() {
        Consumer consumer = consumerFactory.createConsumer(groupId, null);
        consumer.subscribe(Arrays.asList(topics));
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(6000));
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            System.out.println("消费消息:" + consumerRecord.value() + "...." + consumerRecord.key());
        }
        consumer.commitAsync();
        consumer.close();
    }


}
