package kevinmq.server.nameserver;

import kevinmq.server.broker.Broker;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

import java.util.Map;
import java.util.Set;

/**
 * 代表一个 broker 的路由信息，有多个{@code Map<topic,tags>}
 *
 * @author Kevin2
 */
@AllArgsConstructor @EqualsAndHashCode
public class BrokerInfo {
    public Broker broker;
    /**
     * 每个 topic 被封装为一条{@code Map<topic,tags>}
     */
    public Map<String, Set<String>> topicInfo;
}
