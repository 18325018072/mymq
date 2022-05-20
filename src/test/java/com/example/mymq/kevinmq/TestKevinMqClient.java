package com.example.mymq.kevinmq;

import kevinmq.client.consumer.Consumer;
import kevinmq.client.consumer.process.ConsumeStatus;
import kevinmq.client.consumer.process.MessageListener;
import kevinmq.client.producer.Producer;
import kevinmq.client.producer.SendCallback;
import kevinmq.client.producer.res.SendResult;
import kevinmq.message.Message;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Kevin2
 */
public class TestKevinMqClient {
    static int producerNum = 0;
    static int consumerNum = 0;

    public static void main(String[] args) {

        Runnable producerRunnable = new Runnable() {
            @Override
            public void run() {
                //创建Producer
                System.out.println("Producer Created");
                Producer producer1 = new Producer("Producer No." + producerNum++);
                //Producer以同步方式发送信息
                producer1.sendSynchronously(new Message("運命を進め", "衣服", "上衣"));
                producer1.sendSynchronously(new Message("運命を進め".getBytes(StandardCharsets.UTF_8), "衣服", "上衣"));
                //Producer以异步方式发送消息
                producer1.sendAsync(new Message("運命を進め", "衣服", "上衣"), new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.println("异步发送成功：" + sendResult);
                    }

                    @Override
                    public void onFail(Exception e) {
                        System.out.println("异步发送成功");
                    }
                });
                //Producer发送信息，这些topic和tag的消息没有Broker能接受，所以白发了
                producer1.sendSynchronously(new Message("運命を進め".getBytes(StandardCharsets.UTF_8), "衣服", "上衣1"));
                producer1.sendSynchronously(new Message("運命を進め"));
                System.out.println("Producer end");
            }
        };
        ThreadPoolExecutor pool = new ThreadPoolExecutor(10, 20, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<>(5));
        Runnable consumerRunnable = new Runnable() {
            @Override
            public void run() {
                //创建Consumer
                System.out.println("Consumer Created");
                Consumer consumer1 = new Consumer("Consumer No." + consumerNum++);
                //Consumer订阅消息
                consumer1.subscribe("衣服", "上衣");
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
            }
        };
        Thread ct = new Thread(consumerRunnable);
        System.out.println(ct);

        for (int i = 0; i < 10; i++) {
            new Thread(producerRunnable).start();
        }


        for (int i1 = 0; i1 < 10; i1++) {
            StringBuilder stringBuilder = new StringBuilder();stringBuilder.append(5);
        }
        System.out.println(1);
//        pool.shutdown();
        System.out.println(2);
    }


    public static void main() {
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
}
}
