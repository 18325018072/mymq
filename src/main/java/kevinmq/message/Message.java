package kevinmq.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;


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
    private String topic="";
    private String tag="";
    private String keys="";
    private String producerName;

    public Message(String data,String topic, String tag) {
        this.body=data.getBytes(StandardCharsets.UTF_8);
        this.topic = topic;
        this.tag = tag;
    }

    public Message(String data,String topic, String tag, String keys) {
        this.body=data.getBytes(StandardCharsets.UTF_8);
        this.topic = topic;
        this.tag = tag;
        this.keys = keys;
    }

    public Message(String data) {
        this.body=data.getBytes(StandardCharsets.UTF_8);
    }

    public Message(byte[] body, String topic, String tag) {
        this.body = body;
        this.topic = topic;
        this.tag = tag;
    }

    public Message(byte[] body, String topic, String tag, String keys) {
        this.body = body;
        this.topic = topic;
        this.tag = tag;
        this.keys = keys;
    }
}
