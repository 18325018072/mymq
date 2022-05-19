package kevinmq.server.nameserver;

import kevinmq.client.consumer.Consumer;
import kevinmq.client.consumer.data.SubscriptionData;
import kevinmq.server.broker.Broker;
import kevinmq.server.broker.data.ConsumeQueue;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * 命名服务器
 *
 * @author Kevin2
 */
public class NameServer {
    private static final int DEFAULT_BROKER_NUMS = 100;
    /**
     * 注册的每个 Broker 和其路由信息：<br/>
     * {@code HashMap<Broker,HashMap<topic,HashSet<tag>>>}
     */
    private Set<BrokerInfo> brokerInfoSet =new HashSet<>(DEFAULT_BROKER_NUMS);

    /**
     * 单例模式的 NameServer对象
     */
    private static volatile NameServer nameserver;

    /**
     * 饿汉单例模式创建 NameServer对象
     */
    public static NameServer getNameServer() {
        if (nameserver == null) {
            synchronized (nameserver) {
                if (nameserver == null) {
                    nameserver = new NameServer();
                }
            }
        }
        return nameserver;
    }

    /**
     * 接受Broker的心跳：
     * 向该 NameServer 注册一个 Broker 的路由信息;或更新 Broker 路由信息
     * @param broker 要注册或更新信息的 Broker
     */
    public void receiveHeartbeatFromBroker(@NotNull Broker broker) {
        Map<String,Set<String>> newInfo = new HashMap<>(6);

        HashMap<String, HashMap<String, ArrayList<ConsumeQueue>>> topicTable = broker.getBrokerData().getTopicTable();
        Set<String> newTopicSet = topicTable.keySet();
        for (String newTopic : newTopicSet) {
            //对于每个topic
            Set<String> tagSet = topicTable.get(newTopic).keySet();
            newInfo.put(newTopic,tagSet);
        }
        brokerInfoSet.add(new BrokerInfo(broker,newInfo));
    }

    /**
     * 通过 topic、tag 查找 broker
     * @return
     */
    public BrokerInfo findBrokerInfoByTopicTag(@NonNull String topic, String tag){
        for (BrokerInfo broInfo : brokerInfoSet) {
            Set<String> tags = broInfo.topicInfo.get(topic);
            if (tags!=null && tags.contains(tag)) {
                return broInfo;
            }
        }
        return null;
    }

    /**
     * 移除 broker
     */
    public void removeBroker(Broker broker) {
        brokerInfoSet.removeIf(brokerInfo -> brokerInfo.broker==broker);
    }

    /**
     * 接受Consumer的心跳：告诉他从哪些Broker接收
     */
    public Set<BrokerInfo> receiveHeartbeatFromConsumer(Consumer consumer, ConcurrentMap<String, SubscriptionData> subscriptionMap) {
        Set<BrokerInfo> res=new HashSet<>();

        total:for (BrokerInfo brokerInfo : brokerInfoSet) {
            //对每个brokerInfo：这个需要吗？
            Set<String> topics = brokerInfo.topicInfo.keySet();
            Set<String> subTopics = subscriptionMap.keySet();

            for (String topic : topics) {
                Set<String> tags = brokerInfo.topicInfo.get(topic);
                //使用NameServer的每个topic+tag询问是否需要
                if (subTopics.contains(topic)) {
                    //如果有这个topic
                    Set<String> tagsSet = subscriptionMap.get(topic).getTagsSet();
                    for (String tag : tags) {
                        if (tagsSet.contains(tag)) {
                            //如果还有这个tag，命中
                            res.add(brokerInfo);
                            continue total;
                        }
                    }
                }
            }
        }
        return res;
    }
}
