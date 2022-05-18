package kevinmq.client.consumer;

import kevinmq.client.Client;
import kevinmq.client.consumer.data.ConsumerDataManager;
import kevinmq.client.consumer.process.MessageProcessor;
import kevinmq.message.Message;
import kevinmq.server.nameserver.BrokerInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * 消息消费者。
 * 消费形式：Broker push 推。
 * 每隔30s（由ClientConfig中heartbeatBrokerInterval决定）向 broker 发送心跳。
 * 一个 consumer 拥有一个 messageListener，可以订阅多个 topic、tag
 *
 * @author Kevin2
 */
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@Data
public class Consumer extends Client {
    private ConsumerDataManager data = new ConsumerDataManager();
    private MessageProcessor processor;

    public Consumer(String name) {
        data.setConsumerName(name);
        processor = new MessageProcessor(name);
    }

    /**
     * 订阅消息
     *
     * @param subExpression 子表达式，用于筛选。或为tag，或为*
     */
    public void subscribe(String topic, String subExpression) {
        try {
            data.addSubscription(topic, subExpression);
            sendHeartbeatToAllBroker();
        } catch (Exception e) {
            throw new RuntimeException("subscription exception");
        }
    }

    /**
     * 接受来自 broker 的消息
     */
    public void receiveMessage(Message message) {
        processor.process(message);
    }

    /**
     * Consumer 向提供Topic服务的 broker 建立长连接，每隔30s（由ClientConfig中heartbeatBrokerInterval决定）向 broker 发送心跳。
     */
    @Override
    protected void sendHeartbeatToAllBroker() {
        for (BrokerInfo info : brokerInfo) {
            info.broker.receiveHeartBeat(this, data.getSubscriptionMap());
        }
    }
}
