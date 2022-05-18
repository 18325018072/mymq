package kevinmq.server.broker.data;

import kevinmq.message.Message;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * ConsumeQueue （逻辑消费队列）作为消费消息的索引，保存了指定 Topic 下的队列消息在 CommitLog 中的起始物理偏移量 offset ，
 * 消息大小 size 和消息 Tag 的 HashCode 值。 Consumer 即可根据 ConsumeQueue 来查找待消费的消息。
 * @author Kevin2<br />
 * last updated:2022/5/16
 */
@Data@AllArgsConstructor
public class ConsumeQueue {
    /**
     * broker名 + topic名 + queue序号
     */
    private String brokerName;
    private String topic;
    private String queueId;
    /**
     * 该queue归属的CommitLog
     */
    private CommitLog data;
    /**
     * 消息 Tag 的 HashCode 值。实现麻烦，先用String tag吧
     */
    private String tag;

    /**
     * 队列消息在 CommitLog 中的起始物理偏移量 offset
     */
    private int physicOffset;

    public void addMessage(Message msg) {
        data.addMessage(msg,physicOffset);
    }
    public Message removeMessage() throws Exception {return data.removeMessage(physicOffset);}
}
