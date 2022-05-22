package kevinmq.server.broker.solver;

import kevinmq.client.consumer.Consumer;
import kevinmq.server.broker.data.ConsumeQueue;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.concurrent.*;

/**
 * 任务类。负责一个 queue 的 consumer 管理和消息发送
 *
 * @author Kevin2
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueueSender {
    private ConsumeQueue queue;
    private List<Consumer> consumerList;
    private boolean running;

    private static ExecutorService threadPool = new ThreadPoolExecutor(10, 50,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(1024),
            new ThreadFactory() {
                int i = 0;

                @Override
                public Thread newThread(@NotNull Runnable r) {
                    return new Thread(r, "broker_Sender" + i++);
                }
            });

    public void start() {
        running = true;
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                //只要有客户，就一直（阻塞）发送
                while (running) {
                    if (!consumerList.isEmpty()){
                        for (Consumer consumer : consumerList) {
                            try {
                                consumer.receiveMessage(queue.removeMessage());
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        };
        threadPool.submit(runnable);
    }

    /**
     * shutdown一个sender
     */
    public void shutdown() {
        running = false;

    }

    /**
     * shutdown所有sender的线程池
     */
    public static void shutdownAllSenders(){
        threadPool.shutdownNow();
    }
}
