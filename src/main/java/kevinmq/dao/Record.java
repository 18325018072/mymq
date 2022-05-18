package kevinmq.dao;

import kevinmq.message.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 日志记录对象
 * @author Kevin2<br />
 * last updated:2022/5/16
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Record implements Serializable {
    /**
     * 谁
     */
    String who;
    /**
     * 操作
     */
    String action;
    /**
     * 内容
     */
    Object msg;
}