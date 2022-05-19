package kevinmq.server.broker.solver;

import kevinmq.client.consumer.Consumer;
import kevinmq.server.broker.data.ConsumeQueue;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 任务类。负责一个 queue 的 consumer 管理和消息发送
 *
 * @author Kevin2
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueueSender implements Runnable {
    private ConsumeQueue queue;
    private List<Consumer> consumerList;
    private boolean running;

    private static ExecutorService threadPool = new ThreadPoolExecutor(10, 50,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(1024),
            new ThreadPoolExecutor.AbortPolicy());

    @Override
    public void run() {
        //只要有客户，就一直（阻塞）发送
        while (running && !consumerList.isEmpty()) {
            for (Consumer consumer : consumerList) {
                try {
                    consumer.receiveMessage(queue.removeMessage());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void start() {
        running = true;
        threadPool.submit(this);
    }

    public void shutdown() {
        running = false;
    }
}
