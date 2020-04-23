/*******************************************************************************
 * Copyright (C) 2018 - 2019, The YUAN Authors.
 * All Rights Reserved.
 ******************************************************************************/

package com.lhzh.y.test;

import java.util.Date;

import com.aliyun.mq.http.MQClient;
import com.aliyun.mq.http.MQProducer;
import com.aliyun.mq.http.model.TopicMessage;

public class HttpProducerTest {

    public static void main(String[] args) {
        MQClient mqClient = new MQClient(
                // 设置HTTP接入域名（此处以公共云生产环境为例）
                "http://1445960410428144.mqrest.cn-beijing.aliyuncs.com",
                // AccessKey 阿里云身份验证，在阿里云服务器管理控制台创建
                "LTAI4FkcYRQmbCZCFhDoskHn",
                // SecretKey 阿里云身份验证，在阿里云服务器管理控制台创建
                "yDfoa0ZdiynxSYFz2kj7vBGGtdHdHa");

        // 所属的 Topic
        final String topic = "demo";
        // Topic所属实例ID，默认实例为空
        final String instanceId = "MQ_INST_1445960410428144_Bbv5NVGs";

        // 获取Topic的生产者
        MQProducer producer;
        if (instanceId != null && instanceId != "") {
            producer = mqClient.getProducer(instanceId, topic);
        } else {
            producer = mqClient.getProducer(topic);
        }

        try {
            // 循环发送100条消息
            for (int i = 0; i < 100; i++) {
                TopicMessage pubMsg = new TopicMessage(
                        // 消息内容
                        "hello mq!".getBytes(),
                        // 消息标签
                        "TagA");
                // 同步发送消息，只要不抛异常就是成功
                TopicMessage pubResultMsg = producer.publishMessage(pubMsg);

                // 同步发送消息，只要不抛异常就是成功
                System.out.println(new Date() + " Send mq message success. Topic is:" + topic + ", msgId is: " + pubResultMsg.getMessageId()
                        + ", bodyMD5 is: " + pubResultMsg.getMessageBodyMD5());
            }
        } catch (Throwable e) {
            // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理
            System.out.println(new Date() + " Send mq message failed. Topic is:" + topic);
            e.printStackTrace();
        }

        mqClient.close();
    }

}
