package kevinmq.client.producer;

import kevinmq.client.Client;
import kevinmq.server.broker.Broker;
import kevinmq.message.Message;
import kevinmq.server.nameserver.BrokerInfo;
import kevinmq.server.nameserver.NameServer;
import kevinmq.client.producer.res.SendResult;
import kevinmq.client.producer.res.SendStatus;

import java.util.*;

/**
 * 消息发送者
 * @author Kevin2
 */
public class Producer extends Client implements SendMessage{

    private String producerId="Default Producer";

    /**
     * 发送同步消息
     * @return 发送结果
     */
    @Override
    public SendResult sendSynchronously(Message msg) {
        //查找本地的 topic 路由信息
        Broker desBroker = findBrokerByTopicTag(msg.getTopic(),msg.getTag());
        if (desBroker==null){
            //没找到，返回错误信息
            return new SendResult(SendStatus.Broker_NotFound,msg,null);
        }
        //找到目的broker了
        try {
            //同步：等待Broker返回
            return desBroker.receiveMessage(msg, producerId);
        }catch (Exception e){
            //出现异常
            return new SendResult(SendStatus.Send_Error,msg,null);
        }
    }

    /**
     * 发送异步消息
     * @param callback 回调，处理 SendResult 对象
     */
    @Override
    public void sendAsync(Message msg, SendCallback callback) {
        //查找本地的 topic 路由信息
        Broker broker = findBrokerByTopicTag(msg.getTopic(), msg.getTag());
        if (broker==null) {
            //如果没找到broker
            throw new RuntimeException("There is no topic&tag-matched broker, maybe you can register");
        }
        //发送
        broker.receiveMessageAsync(msg, producerId,callback);
    }




}
