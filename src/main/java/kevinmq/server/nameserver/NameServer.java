package kevinmq.server.nameserver;

import kevinmq.server.broker.Broker;
import kevinmq.server.broker.data.ConsumeQueue;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;

import java.util.*;

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
    private List<BrokerInfo> brokerInfo=new ArrayList<>(DEFAULT_BROKER_NUMS);

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
     * 向该 NameServer 注册一个 Broker 的路由信息;或更新 Broker 路由信息
     * @param broker 要注册或更新信息的 Broker
     */
    public void updateWithBroker(@NotNull Broker broker) {
        Map<String,Set<String>> newInfo = new HashMap<>(6);

        HashMap<String, HashMap<String, ArrayList<ConsumeQueue>>> topicTable = broker.getBrokerData().getTopicTable();
        Set<String> newTopicSet = topicTable.keySet();
        for (String newTopic : newTopicSet) {
            //对于每个topic
            Set<String> tagSet = topicTable.get(newTopic).keySet();
            newInfo.put(newTopic,tagSet);
        }
        brokerInfo.add(new BrokerInfo(broker,newInfo));
    }

    /**
     * 通过 topic、tag 查找 broker
     * @return
     */
    public BrokerInfo findBrokerByTopicTag(@NonNull String topic, String tag){
        for (BrokerInfo broInfo : brokerInfo) {
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
        brokerInfo.removeIf(brokerInfo -> brokerInfo.broker==broker);
    }
}
