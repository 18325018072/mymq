package kevinmq.server.broker.data;

import kevinmq.message.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 消息主体以及元数据的存储主体。
 * 存储 Producer 端写入的消息主体内容，消息内容不是定长的。
 * 单个文件大小默认1G，文件名长度为20位，左边补零，剩余为起始偏移量。
 * 消息主要是顺序写入日志文件，当文件满了，写入下一个文件
 *
 * @author Kevin2
 */
@Data
public class CommitLog {
    /**
     * 消息主体以及元数据
     */
    private ArrayList<MessageQueue> data;

    /**
     * 添加一条信息，并写入 msgQueue
     *
     * @param msg          消息
     * @param physicOffset MessageQueue在CommitLog中的物理偏移值
     */
    public void addMessage(Message msg, int physicOffset) {
        //通过偏移值找到MessageQueue
        MessageQueue messageQueue = data.get(physicOffset);
        messageQueue.queue.add(msg);
    }


    /**
     * 从指定mq中取出一条消息
     * @param physicOffset 定位mq的偏移值
     * @return 消息
     */
    public Message removeMessage(int physicOffset) throws InterruptedException {
        MessageQueue messageQueue = data.get(physicOffset);
        return messageQueue.queue.take();
    }

    /**
     * 添加归属一个topic的多个MessageQueue
     * @param queueNum 要添加的queue数量
     * @return MessageQueue在CommitLog中的物理偏移值。若有多个queue，则为初始queue的物理偏移值
     */
    public int addTopicTagQueues(int queueNum, String topic, String tag) {
        int size = data.size();
        for (int i = 0; i < queueNum; i++) {
            data.add(new MessageQueue(new LinkedBlockingQueue<>(),i+"",topic,tag));
        }
        return size;
    }


    /**
     * 封装消息主体以及元数据的内部类。
     * ConsumeQueue是用于找它的索引类
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private class MessageQueue implements Serializable {
        /**
         * 数据
         */
        LinkedBlockingQueue<Message> queue;
        /**
         * 元数据
         */
        String queueId;
        String topic;
        String tag;
    }
}
