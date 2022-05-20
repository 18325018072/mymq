package kevinmq.dao;

import kevinmq.message.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 日志记录对象
 *
 * @author Kevin2<br />
 * last updated:2022/5/16
 */
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
public class Record implements Serializable {
    /**
     * 谁
     */
    public String who;
    /**
     * 操作
     */
    public String action;
    /**
     * 内容
     */
    public Object msg;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("\n\t" + who +"  "+action);
        if (msg!=null) {
            sb.append("\n\t" + msg);
        }
        return sb.toString();
    }
}