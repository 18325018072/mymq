package kevinmq.server.broker.solver;

import kevinmq.client.producer.SendCallback;
import kevinmq.client.producer.res.SendResult;
import kevinmq.client.producer.res.SendStatus;
import kevinmq.dao.Record;
import kevinmq.dao.Store;
import kevinmq.message.Message;
import kevinmq.server.broker.data.BrokerData;
import kevinmq.server.broker.data.ConsumeQueue;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Kevin2
 */
public class ProducerSolver {
    private BrokerData brokerData;
    /**
     * 用于处理异步消息的线程池：长度无限，存活时间3分钟
     */
    private ThreadPoolExecutor threadPoolForAsyncMes = new ThreadPoolExecutor(50, 500,
            3, TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(), new ThreadFactory() {
        int index = 0;
        @Override
        public Thread newThread(@NotNull Runnable r) {
            return new Thread(r, "thread No." + index++);
        }
    });

    /**
     * 接收同步消息
     * <p>
     * msg+producerId 用于形成 record，形成日志<br/>
     * msg+msgQueue 用于存储数据
     * </p>
     */
    public SendResult receiveMessage(Message msg, String producerId) {
        ConsumeQueue queue = brokerData.findOneMessageQueueByTopicTag(msg.getTopic(), msg.getTag());
        if (queue == null) {
            //没有queue与topic和tag匹配
            return new SendResult(SendStatus.Broker_NotFound, msg, null);
        }
        //添加日志
        Store.getStore().save(new Record(producerId, "Send", msg));
        //通过consumeQueue存储数据
        queue.addMessage(msg);
        return new SendResult(SendStatus.Send_OK, msg, queue);
    }

    /**
     * 接受异步消息，用线程池处理
     *
     * @param callback 处理函数
     */
    public void receiveMessageAsync(Message msg, String producerId, SendCallback callback) {
        Runnable runnable = () -> {
            ConsumeQueue queue = brokerData.findOneMessageQueueByTopicTag(msg.getTopic(), msg.getTag());
            if (queue == null) {
                //没有queue与topic和tag匹配
                callback.onFail(new RuntimeException("没有queue与topic和tag匹配"));
                return;
            }
            //添加日志
            Store.getStore().save(new Record(producerId, "Send", msg));
            //通过consumeQueue存储数据
            queue.addMessage(msg);
            callback.onSuccess(new SendResult(SendStatus.Send_OK, msg, queue));
        };
        threadPoolForAsyncMes.execute(runnable);
    }
}
