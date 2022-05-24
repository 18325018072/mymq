package kevinmq.client.consumer;

import kevinmq.client.Client;
import kevinmq.client.consumer.data.ConsumerDataManager;
import kevinmq.client.consumer.process.MessageListener;
import kevinmq.client.consumer.process.MessageProcessor;
import kevinmq.message.Message;
import kevinmq.server.nameserver.BrokerInfo;
import kevinmq.server.nameserver.NameServer;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * 消息消费者。
 * 消费形式：Broker push 推。
 * 每隔30s（由ClientConfig中heartbeatBrokerInterval决定）向 broker 发送心跳。
 * 一个 consumer 拥有一个 messageListener，可以订阅多个 topic、tag
 *
 * @author Kevin2
 */
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@Data
public class Consumer extends Client {
    /**
     * 包括订阅条件、元数据
     */
    private ConsumerDataManager data = new ConsumerDataManager();
    /**
     * 消息处理者，只能用一种方式来处理消息
     */
    private MessageProcessor processor;
    private boolean running = false;
    private ScheduledThreadPoolExecutor heartPool;

    private boolean localBrokerInfoInitialized = false;


    public Consumer(String name) {
        data.setConsumerName(name);
        processor = new MessageProcessor(name);
    }

    /**
     * 订阅消息
     *
     * @param subExpression 子表达式，用于筛选。或为tag，或为*
     */
    public void subscribe(String topic, String subExpression) {
        try {
            data.addSubscription(topic, subExpression);
            //未启动时是无效的，因为不会发送心跳
            sendHeartbeatToAllBroker();
        } catch (Exception e) {
            throw new RuntimeException("subscription exception");
        }
    }

    /**
     * 注册回调函数（处理收到的消息的方法）
     */
    public void registerMessageListener(MessageListener messageListener) {
        processor.registerMessageListener(messageListener);
    }

    /**
     * 接受来自 broker 的消息
     */
    public void receiveMessage(Message message) {
        processor.process(message);
    }

    /**
     * Consumer 向提供Topic服务的 broker 建立长连接，每隔30s（由ClientConfig中heartbeatBrokerInterval决定）向 broker 发送心跳。
     */
    private void sendHeartbeatToAllBroker() {
        for (BrokerInfo info : brokerInfo) {
            info.broker.receiveHeartBeatFromClient(this, data.getSubscriptionMap());
        }
    }

    /**
     * 将自己的订阅信息告诉NameServer，让NameServer返回自己需要的Broker路由信息
     */
    private void sendHeartbeatToNameServer(){
        brokerInfo=NameServer.getNameServer().receiveHeartbeatFromConsumer(this, data.getSubscriptionMap());
        if (brokerInfo == null) {
            shutdown();
        }
    }

    /**
     * 开始心跳
     */
    public void start(){
        System.out.println(data.getConsumerName()+"启动");
        running=true;
        //初始化ThreadPool
        heartPool = new ScheduledThreadPoolExecutor(30,new ThreadFactory() {
            int i;

            @Override
            public Thread newThread(@NotNull Runnable r) {
                return new Thread(r, data.getConsumerName()+"consumer_Heart" + i++);
            }
        });

        //向NameServer发送心跳
        heartPool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (running) {
                    sendHeartbeatToNameServer();
                    localBrokerInfoInitialized=true;
                }
            }
        }, 0, 30, TimeUnit.SECONDS);

        //向所有brokers发心跳
        heartPool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (running) {
                    while (!localBrokerInfoInitialized){
                        //等待向NameServer发送的心跳初始化本地的broker
                    }
                    sendHeartbeatToAllBroker();
                }
            }
        }, 0, 30, TimeUnit.SECONDS);
    }

    /**
     * 停止心跳
     */
    public void shutdown() {
        running=false;
        heartPool.shutdownNow();
        processor.getThreadPool().shutdownNow();
    }
}
