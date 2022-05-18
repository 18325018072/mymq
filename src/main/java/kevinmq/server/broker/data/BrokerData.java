package kevinmq.server.broker.data;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

/**
 * Broker的信息、数据。以及获取接口
 * @author Kevin2
 */
@Data @NoArgsConstructor public class BrokerData {
    /**
     * 消息主体以及元数据 的存储主体
     */
    private CommitLog data = new CommitLog();
    /**
     * topic、tags 对应的路由信息 {@code Map<topic,Map<tag,queues>>}
     */
    private HashMap<String, HashMap<String, ArrayList<ConsumeQueue>>> topicTable = new HashMap<>();
    private String brokerName;
    public BrokerData(String name){
        this.brokerName=name;
    }


    /**
     * 通过 topic、tag 找到 Broker 中对应 MessageQueue[]中随机选择一条返回。<br/>
     * 这里tag不应该传入""。因为tag=""可能对应的是多个queue的一组，擅自修改其中一个queue会导致该tag组的tag不一致
     *
     * @return topic、tag 对应的一条 MessageQueue
     */
    public ConsumeQueue findOneMessageQueueByTopicTag(String topic, String tag) {
        HashMap<String, ArrayList<ConsumeQueue>> tagCqMap = topicTable.get(topic);
        if (tagCqMap == null) {
            //连topic都没有
            return null;
        }
        ArrayList<ConsumeQueue> queues = tagCqMap.get(tag);
        //如果没有tag匹配的，返回null
        if (queues == null) {
            return null;
        }
        //多个ConsumeQueue中选择一个
        int randomIndex = new Random().nextInt(queues.size());
        return queues.get(randomIndex);
    }

    /**
     * 通过 topic、tag 查找<br/> 对应 {@code List<queue>}
     */
    public ArrayList<ConsumeQueue> findAllMessageQueuesByTopicTag(String topic, String tag) {
        HashMap<String, ArrayList<ConsumeQueue>> tagCqMap = topicTable.get(topic);
        return tagCqMap.get(tag);
    }

    /**
     * 通过 topic 查找<br/>所有 tag 的 queues： Map{@code <tag,List<queue>>}
     *
     * @return 某主题对应的 Map{@code <tag,List<queue>>}
     */
    public HashMap<String, ArrayList<ConsumeQueue>> findTagQueuesByTopic(String topic) {
        return topicTable.get(topic);
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
        //若有，返回原topicMap。若没有topic，创建topicMap
        HashMap<String, ArrayList<ConsumeQueue>> topicMap = topicTable.putIfAbsent(topic, new HashMap<>(6));

        if (topicMap.containsKey(tag)) {
            //已存在tag，无需重复添加
            return false;
        } else {
            //数据层：CommitLog添加
            int offset = data.addTopicTagQueues(queueNum, topic, tag);
            //路由消息：添加tag的queues
            ArrayList<ConsumeQueue> tagQueues = new ArrayList<>(queueNum);
            for (int j = 0; j < queueNum; j++) {
                tagQueues.add(new ConsumeQueue(brokerName, topic, j + "",data, tag, offset++));
            }
            topicMap.put(tag, tagQueues);
            topicTable.put(topic, topicMap);
            return true;
        }
    }
}
