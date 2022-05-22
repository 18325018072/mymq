package kevinmq.server.broker;

import kevinmq.client.Client;
import kevinmq.client.consumer.Consumer;
import kevinmq.client.consumer.data.SubscriptionData;
import kevinmq.client.producer.Producer;
import kevinmq.client.producer.SendCallback;
import kevinmq.client.producer.res.SendResult;
import kevinmq.dao.Record;
import kevinmq.dao.Store;
import kevinmq.message.Message;
import kevinmq.server.broker.data.BrokerData;
import kevinmq.server.broker.data.ConsumeQueue;
import kevinmq.server.broker.solver.ConsumerSolver;
import kevinmq.server.broker.solver.ProducerSolver;
import kevinmq.server.nameserver.NameServer;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * 服务器<br/>
 *
 * @author Kevin2
 */
@Data
public class Broker {
    private BrokerData brokerData= new BrokerData("Default");
    private ConsumerSolver consumerSolver = new ConsumerSolver();
    private ProducerSolver producerSolver = new ProducerSolver(brokerData);
    private boolean running = false;
    private ScheduledThreadPoolExecutor threadPool;

    public Broker() {
        NameServer.getNameServer().receiveHeartbeatFromBroker(this);
    }

    public Broker(String brokerName) {
        brokerData.setBrokerName(brokerName);
        NameServer.getNameServer().receiveHeartbeatFromBroker(this);
    }

    /**
     * 接收同步消息
     * <p>
     * msg+producerId 用于形成 record，形成日志<br/>
     * msg+msgQueue 用于存储数据
     * </p>
     */
    public SendResult receiveMessage(Message msg, String producerId) {
        return producerSolver.receiveMessage(msg, producerId);
    }

    /**
     * 接受异步消息，用线程池处理
     *
     * @param callback 处理函数
     */
    public void receiveMessageAsync(Message msg, String producerId, SendCallback callback) {
        producerSolver.receiveMessageAsync(msg, producerId, callback);
    }

    /**
     * 通过 topic、tag 找到 Broker 中对应 MessageQueue[]中随机选择一条返回。<br/>
     * 这里tag不应该传入""。因为tag=""可能对应的是多个queue的一组，擅自修改其中一个queue会导致该tag组的tag不一致
     *
     * @return topic、tag 对应的一条 MessageQueue
     */
    public ConsumeQueue findOneMessageQueueByTopicTag(String topic, String tag) {
        return brokerData.findOneMessageQueueByTopicTag(topic, tag);
    }

    /**
     * 通过 topic、tag 查找<br/> 对应 {@code List<queue>}
     */
    public ArrayList<ConsumeQueue> findAllMessageQueuesByTopicTag(String topic, String tag) {
        HashMap<String, ArrayList<ConsumeQueue>> tagCqMap = brokerData.getTopicTable().get(topic);
        return tagCqMap.get(tag);
    }

    /**
     * 通过 topic 查找<br/>所有 tag 的 queues： Map{@code <tag,List<queue>>}
     *
     * @return 某主题对应的 Map{@code <tag,List<queue>>}
     */
    public HashMap<String, ArrayList<ConsumeQueue>> findTagQueuesByTopic(String topic) {
        return brokerData.getTopicTable().get(topic);
    }

    /**
     * 创建 topic。topic 拥有 tagNums 个 tag，一个tag拥有 num 个 MessageQueue[]。
     * 只初始化了 topic，初始 tag 都为 “”
     *
     * @param queueNum 每个tag的消息队列数量
     * @return true:添加成功
     * false:已存在，故无需重复添加
     */
    public boolean addTopicTag(String topic, String tag, int queueNum) {
        if (brokerData.addTopicTag(topic, tag, queueNum)) {
            //发送心跳给NameServer，以同步路由信息
            sendHeartbeatToNameServer();
            return true;
        } else {
            return false;
        }
    }

    /**
     * 创建 topic，使用默认 tag="",queueNum=6
     *
     * @return true:添加成功 false:已存在，故无需重复添加
     */
    public boolean addTopic(String topic) {
        return addTopicTag(topic, "", 6);
    }

    /**
     * 每隔一定时间（默认30s）发送心跳到 NameServer
     */
    public void sendHeartbeatToNameServer() {
        NameServer.getNameServer().receiveHeartbeatFromBroker(this);
    }

    /**
     * 关闭 broker:<p>停止心跳，并在 NameServer 中注销</p>
     */
    public void shutdown() {
        running = false;
        threadPool.shutdownNow();
        producerSolver.getThreadPoolForAsyncMes().shutdownNow();
        NameServer.getNameServer().removeBroker(this);

        //日志记录
        Store.getStore().save(new Record(brokerData.getBrokerName(), "shutdown", null));
    }

    public void shutdownAllConsumers(){
        consumerSolver.shutdownConsumers();
    }

    /**
     * 接受客户端的心跳
     */
    public void receiveHeartBeatFromClient(Client client, Object data) {
        if (client instanceof Consumer) {
            this.consumerSolver.receiveHeartBeat((Consumer) client, (ConcurrentMap<String, SubscriptionData>) data);
        } else if (client instanceof Producer) {
            //处理来自Producer的心跳……
        }
    }


    /**
     * 开始心跳
     */
    public void start() {
        running = true;
        threadPool = new ScheduledThreadPoolExecutor(3, new ThreadFactory() {
            int i;

            @Override
            public Thread newThread(@NotNull Runnable r) {
                return new Thread(r, "broker-Heart" + i++);
            }
        });
        //向NameServer发送心跳
        threadPool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (running) {
                    sendHeartbeatToNameServer();
                }
            }
        }, 0, 30, TimeUnit.SECONDS);
        //Broker 每隔 10s 扫描所有存活的连接，若某个连接2分钟内没有发送心跳数据，则关闭连接；
        // 并向该 Consumer Group 的所有 Consumer 发出通知，Group 内的Consumer重新分配队列，然后继续消费。
        threadPool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                consumerSolver.countDown();
                consumerSolver.checkHp();
            }
        }, 0, 10, TimeUnit.SECONDS);
        //日志记录
        Store.getStore().save(new Record(brokerData.getBrokerName(), "启动", null));
    }

}
