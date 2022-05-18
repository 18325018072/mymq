package kevinmq.client.producer.res;

import kevinmq.server.broker.data.ConsumeQueue;
import kevinmq.message.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 同步发送结果
 * @author Kevin2
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SendResult {
    private SendStatus status;
    private Message message;
    private ConsumeQueue consumeQueue;
}
