package kevinmq.client.consumer.process;

import kevinmq.dao.Record;
import kevinmq.dao.Store;
import kevinmq.message.Message;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 消息处理者，只能用一种方式来处理消息
 *
 * @author Kevin2
 */
@Data
@NoArgsConstructor
public class MessageProcessor {
    /**
     * 处理消息的线程池
     */
    private ThreadPoolExecutor threadPool = new ThreadPoolExecutor(20, 40,
            30, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100),
            new ThreadFactory() {
                int i = 0;

                @Override
                public Thread newThread(@NotNull Runnable r) {
                    return new Thread(r, "consumer_processor" + i++);
                }
            });
    /**
     * 消息监听处理
     */
    private MessageListener messageListener;
    private String consumerName;
    private final int PROCESSOR_BUSY_LIMIT = 10;

    public MessageProcessor(String consumerName) {
        this.consumerName = consumerName;
    }

    /**
     * 注册消息监听
     */
    public void registerMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }

    /**
     * 取出一个线程，来消费消息
     */
    public void process(Message message) {
        threadPool.execute(() -> {
            //消费
            ConsumeStatus res = messageListener.consumeMessage(message);
            //消费结果处理
            if (res == ConsumeStatus.Consume_Fail) {
                Store.getStore().save(new Record(consumerName, "Fail to consume", message));
            } else if (res == ConsumeStatus.Consume_Success) {
                Store.getStore().save(new Record(consumerName, "consumed", message));
            }
            if (threadPool.getQueue().size() > PROCESSOR_BUSY_LIMIT) {
                //TODO 消息处理压力较大,让broker慢一点
            }
        });
    }
}
