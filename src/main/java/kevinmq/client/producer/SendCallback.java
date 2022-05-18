package kevinmq.client.producer;

import kevinmq.client.producer.res.SendResult;

/**
 * 异步发送消息的回调函数
 */
public interface SendCallback {
    /**
     * 成功的回调
     * @param sendResult 返回的结果
     */
    void onSuccess(SendResult sendResult);

    /**
     * 失败的回调
     * @param e 异常
     */
    void onFail(Exception e);
}
