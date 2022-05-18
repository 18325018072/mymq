package kevinmq.client.consumer.process;

/**
 * 消费结果状态
 * @author Kevin2
 */
public enum ConsumeStatus {
    /**
     * 成功
     */
    Consume_Success,
    /**
     * 处理器有点忙
     */
    Processor_Little_Busy,
    /**
     * 失败
     */
    Consume_Fail
}
