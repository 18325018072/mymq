package kevinmq.client.consumer.process;

import kevinmq.message.Message;

/**
 * 消息处理监听
 * @author Kevin2
 */
public interface MessageListener {
    /**
     * 消费消息
     */
    ConsumeStatus consumeMessage(Message message);
}
