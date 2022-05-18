package kevinmq.client;

import kevinmq.server.broker.Broker;
import kevinmq.server.nameserver.BrokerInfo;
import kevinmq.server.nameserver.NameServer;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * 客户端，包括 Producer 和 Consumer
 * @author Kevin2
 */
@Data public class Client {
    /**
     * 本地缓存的 topic 路由信息
     */
    protected final List<BrokerInfo> brokerInfo=new ArrayList<>();

    /**
     * Consumer要发送 Set<SubscriptionData> subscriptionDataSet 订阅消息。
     * Producer发送信息支持事务，就不发送了
     */
    protected void sendHeartbeatToAllBroker(){}

    /**
     * 寻找 topic、tag 对应的 broker，找不到则返回null
     */
    public Broker findBrokerByTopicTag(String topic, String tag){
        Broker broker = findLocalBrokerByTopicTag(topic,tag);
        if (broker==null) {
            //本地找不到，去NameServer找
            BrokerInfo brokerInfo = NameServer.getNameServer().findBrokerByTopicTag(topic, tag);
            if (brokerInfo!=null){
                //本地没找到，而nameserver找到了，则添加到本地缓存
                this.brokerInfo.add(brokerInfo);
                broker=brokerInfo.broker;
            }
        }
        return broker;
    }

    /**
     * 在本地缓存搜索 topic、tag 匹配的 broker
     */
    public Broker findLocalBrokerByTopicTag(String topic, String tag) {
        for (BrokerInfo info : brokerInfo) {
            Set<String> tags = info.topicInfo.get(topic);
            if (tags!=null&&tags.contains(tag)) {
                return info.broker;
            }
        }
        return null;
    }
}
