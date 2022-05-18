package kevinmq.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;


/**
 * 消息
 * @author Kevin2<br />
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Message implements Serializable {
    private int flag=0;
    private byte[] body;
    private String topic;
    private String tag="";
    private String keys="";
    private String producerId;
}
