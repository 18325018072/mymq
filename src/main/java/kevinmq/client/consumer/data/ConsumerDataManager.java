package kevinmq.client.consumer.data;

import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 包括订阅条件、元数据
 *
 * @author Kevin2
 */
@Data
public class ConsumerDataManager {
    /**
     * 订阅条件：{@code map<topic,SubscriptionData>}
     */
    private ConcurrentMap<String, SubscriptionData> subscriptionMap = new ConcurrentHashMap<>();
    private String consumerName = "Default Consumer";

    public void addSubscription(String topic, String subExpression) {
        SubscriptionData topicSubs = subscriptionMap.putIfAbsent(topic, new SubscriptionData(topic, subExpression));
        topicSubs.addSubscription(subExpression);
    }
}
