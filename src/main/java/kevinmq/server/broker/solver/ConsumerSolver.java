package kevinmq.server.broker.solver;

import kevinmq.client.consumer.Consumer;
import kevinmq.client.consumer.data.SubscriptionData;
import kevinmq.server.broker.Broker;
import kevinmq.server.broker.data.BrokerData;
import kevinmq.server.broker.data.ConsumeQueue;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

/**
 * @author Kevin2
 */
public class ConsumerSolver {
    /**
     * Consumer 订阅注册表
     * {@code 对应关系：Consumer-list<topic,tag>-list<queue>}
     */
    private Map<Consumer, List<ConsumeQueue>> subTable=new HashMap<>();
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
        flushSenderTable(consumer);
    }

    /**
     * 更新senderTable
     *
     * @param consumer
     */
    private void flushSenderTable(Consumer consumer) {
        //移除senders的退休成员：consumer不再需要的占位
        Collection<QueueSender> senders = senderTable.values();
        for (QueueSender sender : senders) {
            List<Consumer> senderConsumerList = sender.getConsumerList();
            //如果某个sender包括该consumer，判断是否保留
            if (senderConsumerList.contains(consumer) && !subTable.get(consumer).contains(sender.getQueue())) {
                //不该继续存在
                senderConsumerList.remove(consumer);
            }
        }
        //根据allSubQueues除去senders中无consumer的queue
        Collection<List<ConsumeQueue>> listCollection = subTable.values();
        List<ConsumeQueue> allSubQueues = new ArrayList<>();
        for (List<ConsumeQueue> queueList : listCollection) {
            allSubQueues.addAll(queueList);
        }
        senderTable.forEach(new BiConsumer<ConsumeQueue, QueueSender>() {
            @Override
            public void accept(ConsumeQueue queue, QueueSender sender) {
                if (!allSubQueues.contains(queue)) {
                    //这个queue，已经没有consumer订阅了
                    sender.shutdown();
                    senderTable.remove(queue);
                }
            }
        });
        //根据allSubQueues，添加senders的新成员
        Set<ConsumeQueue> senderQueues = senderTable.keySet();
        for (ConsumeQueue queue : allSubQueues) {
            if (!senderQueues.contains(queue)) {
                //原来没有该queue，创建该sender
                ArrayList<Consumer> consumerList = new ArrayList<>();
                consumerList.add(consumer);
                QueueSender newSender = new QueueSender(queue, consumerList, false);
                senderTable.put(queue, newSender);
                //启动sender
                newSender.start();
            } else {
                //原来有该queue。若没有该consumer则添加consumer
                List<Consumer> consumerList = senderTable.get(queue).getConsumerList();
                if (!consumerList.contains(consumer)) {
                    consumerList.add(consumer);
                }
            }
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
            //对于每一个topic
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
    }

}
