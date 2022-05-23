package kevinmq.client.producer;

import kevinmq.client.Client;
import kevinmq.dao.Record;
import kevinmq.dao.Store;
import kevinmq.server.broker.Broker;
import kevinmq.message.Message;
import kevinmq.client.producer.res.SendResult;
import kevinmq.client.producer.res.SendStatus;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * 消息发送者
 * 没有绑定关系，每次发送前都要查询。
 * Producer 基本无状态
 *
 * @author Kevin2
 */
@NoArgsConstructor
@AllArgsConstructor
public class ProducerImpl extends Client implements Producer {

    private String producerName = "Default Producer";

    /**
     * 发送同步消息
     *
     * @return 发送结果
     */
    @Override
    public SendResult sendSynchronously(Message msg) {
        msg.setProducerName(producerName);
        //查找本地的 topic 路由信息
        Broker desBroker = findBrokerByTopicTag(msg.getTopic(), msg.getTag());
        if (desBroker == null) {
            //没找到，返回错误信息
            Store.getStore().save(new Record(producerName,"Fail to send",SendStatus.Broker_NotFound));
            return new SendResult(SendStatus.Broker_NotFound, msg, null);
        }
        //找到目的broker了
        try {
            //同步：等待Broker返回
            SendResult sendResult = desBroker.receiveMessage(msg, producerName);
            Store.getStore().save(new Record(producerName,"Send",msg));
            return sendResult;
        } catch (Exception e) {
            //出现异常
            return new SendResult(SendStatus.Send_Error, msg, null);
        }
    }

    /**
     * 发送异步消息
     *
     * @param callback 回调，处理 SendResult 对象
     */
    @Override
    public void sendAsync(Message msg, SendCallback callback) {
        //查找本地的 topic 路由信息
        Broker broker = findBrokerByTopicTag(msg.getTopic(), msg.getTag());
        if (broker == null) {
            //如果没找到broker
            Store.getStore().save(new Record(producerName,"Fail to sendAsync",SendStatus.Broker_NotFound));
            return ;
        }
        //发送
        broker.receiveMessageAsync(msg, producerName, callback);
        Store.getStore().save(new Record(producerName,"sendAsync",msg));
    }


}
