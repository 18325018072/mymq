package kevinmq.server.broker.solver;

import kevinmq.client.consumer.Consumer;
import kevinmq.client.consumer.data.SubscriptionData;
import kevinmq.server.broker.Broker;
import kevinmq.server.broker.data.BrokerData;
import kevinmq.server.broker.data.ConsumeQueue;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

/**
 * 处理Broker的consumer相关事务
 * @author Kevin2
 */
public class ConsumerSolver {
    /**
     * Consumer 订阅注册表
     * {@code 对应关系：Consumer-list<topic,tag>-list<queue>}
     */
    private Map<Consumer, List<ConsumeQueue>> subTable=new HashMap<>();
    private Map<Consumer,Integer> consumerHP=new HashMap<>();
    /**
     * 所有发送者，与 queue 一一对应
     */
    private Map<ConsumeQueue, QueueSender> senderTable=new HashMap<>();
    private BrokerData brokerData;

    /**
     * 接受来自Consumer的心跳，更新订阅注册表和senders信息
     */
    public void receiveHeartBeat(Consumer consumer, ConcurrentMap<String, SubscriptionData> data) {
        //解析data，更新注册表
        flushSubTable(consumer, data);
        //调整senders
        flushSenderTable();
        //心跳记录❥(^_-)
        consumerHP.putIfAbsent(consumer,0);
        consumerHP.replace(consumer,0);
    }

    /**
     * 更新senderTable
     */
    private void flushSenderTable() {
        //1.移除senders的退休成员：consumer不再需要的占位
        removeUselessConsumerFromSender();
        //2.根据allSubQueues除去senders中无consumer的queue
        Set<ConsumeQueue> senderQueues = senderTable.keySet();

        //所有被订阅的queue
        List<ConsumeQueue> allSubQueues = new ArrayList<>();
        for (List<ConsumeQueue> queueList : subTable.values()) {
            allSubQueues.addAll(queueList);
        }

        for (ConsumeQueue queue : senderQueues) {
            if (!allSubQueues.contains(queue)) {
                //这个queue，已经没有consumer订阅了
                senderTable.get(queue).shutdown();
                senderTable.remove(queue);
            }
        }

        //3.根据allSubQueues，添加senders的新成员
        for (ConsumeQueue queue : allSubQueues) {
            if (!senderQueues.contains(queue)) {
                //原来没有该queue，创建该sender
                ArrayList<Consumer> consumerList = new ArrayList<>();
                for (Consumer consumer : subTable.keySet()) {
                    if (subTable.get(consumer).contains(queue)) {
                        //扫描每个consumer的订阅queue，若需要这个queue
                        consumerList.add(consumer);
                    }
                }
                QueueSender newSender = new QueueSender(queue, consumerList, false);
                senderTable.put(queue, newSender);
                //启动sender
                newSender.start();
            } else {
                //原来有该queue。若没有该consumer则添加consumer
                List<Consumer> consumerList = senderTable.get(queue).getConsumerList();
                for (Consumer consumer : subTable.keySet()) {
                    if (subTable.get(consumer).contains(queue)) {
                        //扫描每个consumer的订阅queue，若需要这个queue
                        if (!consumerList.contains(consumer)) {
                            consumerList.add(consumer);
                        }
                    }
                }
            }
        }
    }

    /**
     * 移除senders中 consumer 不再需要的占位
     */
    private void removeUselessConsumerFromSender() {
        Collection<QueueSender> senders = senderTable.values();
        for (QueueSender sender : senders) {
            List<Consumer> senderConsumerList = sender.getConsumerList();
            //如果某个sender包括该consumer，判断是否保留
            senderConsumerList.removeIf(consumer -> (!subTable.containsKey(consumer)||subTable.get(consumer).contains(sender.getQueue())));
        }
    }

    /**
     * 更新订阅注册表
     *
     * @param consumer 消费者
     * @param data     订阅数据{@code Map<topic,subData>}
     */
    private void flushSubTable(Consumer consumer, ConcurrentMap<String, SubscriptionData> data) {
        List<ConsumeQueue> subQueues = new ArrayList<>();
        data.forEach((s, subscriptionData) -> {
            //对于来自consumer的每一个topic
            Set<String> tagsSet = subscriptionData.getTagsSet();
            for (String tag : tagsSet) {
                //对于每一个tag
                //找到对应broker
                Broker broker = consumer.findLocalBrokerByTopicTag(s, tag);
                //在broker中找到对应queues
                ArrayList<ConsumeQueue> tagQueues = broker.findAllMessageQueuesByTopicTag(s, tag);
                subQueues.addAll(tagQueues);
            }
        });
        subTable.put(consumer, subQueues);
        consumerHP.put(consumer,0);
    }

    public void countDown(){
        consumerHP.replaceAll(new BiFunction<Consumer, Integer, Integer>() {
            @Override
            public Integer apply(Consumer consumer, Integer integer) {
                return integer-1;
            }
        });
    }

    /**
     * Broker 每隔 10s 扫描所有存活的连接，若某个连接2分钟内没有发送心跳数据，则关闭连接
     */
    public void checkHp() {
        Set<Consumer> consumers = consumerHP.keySet();
        for (Consumer consumer : consumers) {
            if (consumerHP.get(consumer)<-12) {
                consumerHP.remove(consumer);
                subTable.remove(consumer);
                flushSenderTable();
            }
        }
    }

    public void shutdownConsumers(){
        for (Consumer consumer : subTable.keySet()) {
            if (consumer!=null) {
                consumer.shutdown();
            }
        }
    }
}
