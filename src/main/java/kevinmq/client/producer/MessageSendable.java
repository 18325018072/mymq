package kevinmq.client.producer;

import kevinmq.message.Message;
import kevinmq.client.producer.res.SendResult;

/**
 * 发送消息功能
 * @author 周宇科
 */
public interface MessageSendable {
    /**
     * 同步发送消息
     * @param msg 消息
     * @return
     */
    SendResult sendSynchronously(Message msg);

    /**
     * 异步发送消息
     * @param msg 消息
     * @param callback 回调，处理 SendResult 对象
     */
    void  sendAsync(Message msg,SendCallback callback);
}
