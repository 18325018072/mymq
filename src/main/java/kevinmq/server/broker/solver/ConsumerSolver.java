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
    private Map<Consumer, List<ConsumeQueue>> subTable;
    /**
     * 所有发送者，与 queue 一一对应
     */
    private Map<ConsumeQueue, QueueSender> senders;
    private BrokerData brokerData;

    /**
     * 接受来自Consumer的心跳
     */
    public void receiveHeartBeat(Consumer consumer, ConcurrentMap<String, SubscriptionData> data) {
        List<ConsumeQueue> subQueues=new ArrayList<>();
        //解析data，更新注册表
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
        subTable.put(consumer,subQueues);
        //调整senders
        Collection<List<ConsumeQueue>> listCollection = subTable.values();
        List<ConsumeQueue> allSubQueues=new ArrayList<>();
        for (List<ConsumeQueue> queueList : listCollection) {
            allSubQueues.addAll(queueList);
        }
        //根据allSubQueues去除senders中无效的queue
        senders.forEach(new BiConsumer<ConsumeQueue, QueueSender>() {
            @Override
            public void accept(ConsumeQueue queue, QueueSender sender) {
                if (!allSubQueues.contains(queue)) {
                    sender.shutdown();
                    senders.remove(queue);
                }
            }
        });
        //根据allSubQueues，完善senders
        Set<ConsumeQueue> senderQueues = senders.keySet();
        for (ConsumeQueue queue : allSubQueues) {
            if (!senderQueues.contains(queue)) {
                //原来没有该queue
                ArrayList<Consumer> consumerList = new ArrayList<>();
                consumerList.add(consumer);
                senders.put(queue,new QueueSender(queue,consumerList));
            }else {
                //原来有该queue
                
            }
        }
    }

    //发送消息：
    //运营线程池，线程一对一监控queue，并发送存储的订阅消息：
    //线程1：监视queue1，有则发
    //线程2：监视queue2，有则发

}
