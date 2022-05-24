package com.example.mymq.kevinmq;

import kevinmq.client.consumer.Consumer;
import kevinmq.client.consumer.process.ConsumeStatus;
import kevinmq.client.consumer.process.MessageListener;
import kevinmq.client.producer.Producer;
import kevinmq.client.producer.ProducerImpl;
import kevinmq.client.producer.SendCallback;
import kevinmq.client.producer.res.SendResult;
import kevinmq.message.Message;
import kevinmq.server.broker.Broker;
import kevinmq.server.nameserver.NameServer;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Kevin2
 */
public class TestKevinMqServer {
    static int producerNum = 0;
    static int consumerNum = 0;
    private static ThreadPoolExecutor pool;

    public static void main(String[] args) {
        //开启NameServer
        NameServer.getNameServer().start();
        //创建Broker
        Broker broker1 = new Broker("Broker初号机");
        //开启Broker仓库
        broker1.addTopicTag("衣服", "上衣", 6);
        //启动Broker
        broker1.start();
        System.out.println("启动成功");
        while (true) {
            Scanner scanner = new Scanner(System.in);
            String s = scanner.nextLine();
            if ("p".equals(s)) {
                ProducerGo();
            }
            if ("c".equals(s)) {
                for (int i = 0; i < 10; i++) {
                    ConsumerGo();
                }
            }
            if ("a".equals(s)) {
                ProducerGo();
                ConsumerGo();
            }
            if ("q".equals(s)) {
                //关闭NameServer
                if (pool != null) {
                    pool.shutdownNow();
                }
                NameServer.getNameServer().shutdown();
                System.out.println("退出");
                return;
            }
        }
    }


    public static void ConsumerGo() {
        Runnable consumerRunnable = new Runnable() {

            @Override
            public void run() {
                //创建Consumer
                Consumer consumer1 = new Consumer("Consumer No." + consumerNum++);
                //Consumer订阅消息
                consumer1.subscribe("衣服", "上衣");
                //Consumer注册回调函数（处理收到的消息的方法）
                consumer1.registerMessageListener(new MessageListener() {
                    @Override
                    public ConsumeStatus consumeMessage(Message message) {
                        return ConsumeStatus.Consume_Success;
                    }
                });
                //启动Consumer（启动心跳）
                consumer1.start();
            }
        };
        new Thread(consumerRunnable).start();
    }

    public static void ProducerGo() {
        Runnable producerRunnable = new Runnable() {
            @Override
            public void run() {
                //创建Producer
                Producer producer1 = new ProducerImpl("Producer No." + producerNum++);
                //Producer以同步方式发送信息
                for (int i = 0; i < 100; i++) {
                    producer1.sendSynchronously(new Message("一番運命を進め", "衣服", "上衣"));
                    producer1.sendSynchronously(new Message("に運命を進め".getBytes(StandardCharsets.UTF_8), "衣服", "上衣"));
                    //Producer以异步方式发送消息
                    producer1.sendAsync(new Message("异步の運命を進め", "衣服", "上衣"), new SendCallback() {
                        @Override
                        public void onSuccess(SendResult sendResult) {
                            //异步发送成功
                        }
                        @Override
                        public void onFail(Exception e) {
                            System.out.println("异步发送失败");
                        }
                    });
                }
                //Producer发送信息，这些topic和tag的消息没有Broker能接受，所以白发了
//                producer1.sendSynchronously(new Message("１運命を進め".getBytes(StandardCharsets.UTF_8), "衣服", "上衣1"));
//                producer1.sendSynchronously(new Message("２運命を進め"));
            }
        };
        pool = new ThreadPoolExecutor(10, 20000,
                5, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        for (int i = 0; i < 10000; i++) {
            pool.execute(producerRunnable);
        }
    }
}
