package kevinmq.server.broker.solver;

import kevinmq.client.consumer.Consumer;
import kevinmq.server.broker.data.ConsumeQueue;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

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

    /**
     * 为对应 queue 添加一个consumer
     */
    private void addConsumer(Consumer consumer) {
        consumerList.add(consumer);
    }

    @Override
    public void run() {
        while (true){
            for (Consumer consumer : consumerList) {
                try {
                    consumer.receiveMessage(queue.removeMessage());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void shutdown() {

    }
}
