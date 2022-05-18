package kevinmq.client.consumer.data;

import lombok.Data;

import java.util.HashSet;
import java.util.Set;

/**
 * 消费者的关于一个 topic 的订阅条件
 *
 * @author Kevin2
 */
@Data public class SubscriptionData {
    private String topic;
    private String subExpression;
    private final String ALL_SUB = "*";
    private Set<String> tagsSet = new HashSet<>();

    /**
     * 构造一个订阅条件
     * @param topic 主题
     * @param subExpression 订阅表达式
     */
    public SubscriptionData(String topic, String subExpression) {
        this.topic = topic;
        this.subExpression = subExpression;
        solveSubExpression(subExpression);
    }

    /**
     * 添加订阅条件
     * @param subExpression
     */
    public void addSubscription(String subExpression){
        solveSubExpression(subExpression);
        this.subExpression+="||"+subExpression;
    }
    /**
     * 解析订阅表达式，自动加入tagsSet中
     * @param subExpression
     */
    private void solveSubExpression(String subExpression){
        if (subExpression == null || subExpression.equals(ALL_SUB) || subExpression.length() == 0) {
            //说明要订阅全部,将三种情况下，subExpression统一设为ALL_SUB
            subExpression = ALL_SUB;
        } else {
            //subExpression没那么简单，就需要解析了
            String[] tags = subExpression.split("\\|\\|");
            for (String tag : tags) {
                if (tag.length() > 0) {
                    tag = tag.trim();
                    if (tag.length() > 0) {
                        tagsSet.add(tag.trim());
                    }
                }
            }
        }
    }
}
