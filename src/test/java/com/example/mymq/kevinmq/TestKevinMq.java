package com.example.mymq.kevinmq;

import kevinmq.client.consumer.Consumer;
import kevinmq.client.consumer.process.ConsumeStatus;
import kevinmq.client.consumer.process.MessageListener;
import kevinmq.client.producer.Producer;
import kevinmq.client.producer.SendCallback;
import kevinmq.client.producer.res.SendResult;
import kevinmq.dao.Record;
import kevinmq.dao.Store;
import kevinmq.message.Message;
import kevinmq.server.broker.Broker;
import kevinmq.server.nameserver.NameServer;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;

/**
 * KevinMQ的测试类
 * @author Kevin2
 */
public class TestKevinMq {
    @Test
    public void testMq(){

        //开启NameServer
        NameServer.getNameServer().start();
        //创建Broker
        Broker broker1 = new Broker("Broker初号机");
        //开启Broker仓库
        broker1.addTopicTag("衣服","上衣",6);
        //启动Broker
        broker1.start();


        //创建Producer
        Producer producer1=new Producer("Producer初号机");
        //Producer以同步方式发送信息
        producer1.sendSynchronously(new Message("運命を進め","衣服","上衣"));
        producer1.sendSynchronously(new Message("運命を進め".getBytes(StandardCharsets.UTF_8),"衣服","上衣"));
        //Producer以异步方式发送消息
        producer1.sendAsync(new Message("運命を進め", "衣服", "上衣"), new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("异步发送成功："+sendResult);
            }

            @Override
            public void onFail(Exception e) {
                System.out.println("异步发送成功");
            }
        });
        //Producer发送信息，这些topic和tag的消息没有Broker能接受，所以白发了
        producer1.sendSynchronously(new Message("運命を進め".getBytes(StandardCharsets.UTF_8),"衣服","上衣1"));
        producer1.sendSynchronously(new Message("運命を進め"));

        //创建Consumer
        Consumer consumer1 = new Consumer("Consumer初号机");
        //Consumer订阅消息
        consumer1.subscribe("衣服","上衣");
        //Consumer注册回调函数（处理收到的消息的方法）
        consumer1.registerMessageListener(new MessageListener() {
            @Override
            public ConsumeStatus consumeMessage(Message message) {
                System.out.println(new String(message.getBody()));
                return ConsumeStatus.Consume_Success;
            }
        });
        //启动Consumer（启动心跳）
        consumer1.start();

        //关闭NameServer
        NameServer.getNameServer().shutdown();
        System.out.println(1);
    }

}
