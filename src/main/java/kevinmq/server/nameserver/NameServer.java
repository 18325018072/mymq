package kevinmq.server.nameserver;

import kevinmq.client.consumer.Consumer;
import kevinmq.client.consumer.data.SubscriptionData;
import kevinmq.dao.Record;
import kevinmq.dao.Store;
import kevinmq.server.broker.Broker;
import kevinmq.server.broker.data.ConsumeQueue;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * 命名服务器
 *
 * @author Kevin2
 */
public class NameServer {
    private static final int DEFAULT_BROKER_NUMS = 100;
    /**
     * 注册的每个 Broker 和其路由信息：<br/>
     * {@code HashMap<Broker,HashMap<topic,HashSet<tag>>>}
     */
    private Set<BrokerInfo> brokerInfoSet = new HashSet<>(DEFAULT_BROKER_NUMS);
    private Map<BrokerInfo, Integer> brokerHp = new HashMap<>();

    /**
     * 单例模式的 NameServer对象
     */
    private static volatile NameServer nameserver = null;
    private boolean running;
    private ScheduledThreadPoolExecutor threadPool;

    /**
     * 饿汉单例模式创建 NameServer对象
     */
    public static NameServer getNameServer() {
        if (nameserver == null) {
            synchronized (NameServer.class) {
                if (nameserver == null) {
                    nameserver = new NameServer();
                }
            }
        }
        return nameserver;
    }

    /**
     * 接受Broker的心跳：
     * 向该 NameServer 注册一个 Broker 的路由信息;或更新 Broker 路由信息
     *
     * @param broker 要注册或更新信息的 Broker
     */
    public void receiveHeartbeatFromBroker(@NotNull Broker broker) {
        //解析路由信息
        Map<String, Set<String>> newInfo = new HashMap<>(6);
        HashMap<String, HashMap<String, ArrayList<ConsumeQueue>>> topicTable = broker.getBrokerData().getTopicTable();
        Set<String> newTopicSet = topicTable.keySet();
        for (String newTopic : newTopicSet) {
            //对于每个topic
            Set<String> tagSet = topicTable.get(newTopic).keySet();
            newInfo.put(newTopic, tagSet);
        }
        //更新路由信息
        for (BrokerInfo brokerInfo : brokerInfoSet) {
            if (brokerInfo.broker == broker) {
                brokerInfo.topicInfo = newInfo;
                //心跳记录❥(^_-)
                brokerHp.replace(brokerInfo, 0);
                return;
            }
        }
        //如果没有brokerInfo，则新建
        BrokerInfo newBrokerInfo = new BrokerInfo(broker, newInfo);
        brokerInfoSet.add(newBrokerInfo);
        brokerHp.putIfAbsent(newBrokerInfo, 0);
    }

    /**
     * 通过 topic、tag 查找 broker
     *
     * @return
     */
    public BrokerInfo findBrokerInfoByTopicTag(@NonNull String topic, String tag) {
        for (BrokerInfo broInfo : brokerInfoSet) {
            Set<String> tags = broInfo.topicInfo.get(topic);
            if (tags != null && tags.contains(tag)) {
                return broInfo;
            }
        }
        return null;
    }

    /**
     * 移除 broker
     */
    public void removeBroker(Broker broker) {
        brokerInfoSet.removeIf(brokerInfo -> brokerInfo.broker == broker);
    }

    /**
     * 接受Consumer的心跳：告诉他从哪些Broker接收
     */
    public Set<BrokerInfo> receiveHeartbeatFromConsumer(Consumer consumer, ConcurrentMap<String, SubscriptionData> subscriptionMap) {
        if (!running) {
            return null;
        }
        Set<BrokerInfo> res = new HashSet<>();

        total:
        for (BrokerInfo brokerInfo : brokerInfoSet) {
            //对每个brokerInfo：这个需要吗？
            Set<String> topics = brokerInfo.topicInfo.keySet();
            Set<String> subTopics = subscriptionMap.keySet();

            for (String topic : topics) {
                Set<String> tags = brokerInfo.topicInfo.get(topic);
                //使用NameServer的每个topic+tag询问是否需要
                if (subTopics.contains(topic)) {
                    //如果有这个topic
                    Set<String> tagsSet = subscriptionMap.get(topic).getTagsSet();
                    for (String tag : tags) {
                        if (tagsSet.contains(tag)) {
                            //如果还有这个tag，命中
                            res.add(brokerInfo);
                            continue total;
                        }
                    }
                }
            }
        }
        return res;
    }

    /**
     * 开启 NameServer，开始检测broker心跳
     */
    public void start() {
        running = true;
        threadPool = new ScheduledThreadPoolExecutor(30, new ThreadFactory() {
            int i;

            @Override
            public Thread newThread(@NotNull Runnable r) {
                return new Thread(r, "NameServer-Check" + i++);
            }
        });

        //Name Server 每隔 10s 扫描所有存活 broker 的连接
        // 如果 Name Server 超过2分钟没有收到心跳，则 Name Server 从路由注册表中将其移除该 Broker 的连接。
        threadPool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                //生命减一
                brokerHp.replaceAll(new BiFunction<BrokerInfo, Integer, Integer>() {
                    @Override
                    public Integer apply(BrokerInfo brokerInfo, Integer integer) {
                        return integer - 1;
                    }
                });
                //检查
                Set<BrokerInfo> brokerInfoSet = brokerHp.keySet();
                for (BrokerInfo brokerInfo : brokerInfoSet) {
                    if (brokerHp.get(brokerInfo) < -12) {
                        brokerHp.remove(brokerInfo);
                        brokerInfoSet.remove(brokerInfo);
                    }
                }
            }
        }, 0, 10, TimeUnit.SECONDS);
        //Todo
//        threadPool.scheduleAtFixedRate(new Runnable() {
//            @Override
//            public void run() {
//                System.out.println("NameServer Running");
//            }
//        }, 0, 2, TimeUnit.SECONDS);
        Store.getStore().save(new Record("NameServer", "启动", null));
    }

    /**
     * 关闭 NameServer
     */
    public void shutdown() {
        running = false;
        threadPool.shutdownNow();
        for (BrokerInfo brokerInfo : brokerInfoSet) {
            brokerInfo.broker.shutdownAllConsumers();
            brokerInfo.broker.shutdown();
        }
        Store.getStore().save(new Record("NameServer", "shutdown", "---------------------------"));
        Store.getStore().flush();
    }
}
