package kevinmq.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import redis.clients.jedis.JedisPooled;

/**
 * Dao层的日志类
 *
 * @author Kevin2
 * last updated:2022/5/16
 */
public class RedisStore implements Store {

    /**
     * 存储前，最多缓存多少条消息
     */
    private final int MESSAGE_CACHE_MAXSIZE = 3000;
    /**
     * 缓存容器
     */
    private volatile List<Record> messageCache = Collections.synchronizedList(new ArrayList<>());
    protected static volatile Store storeInstance;
    private static String host;
    private static int port;
    private static DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");


    /**
     * 饿汉单例模式创建 Store对象
     *
     * @return Store对象
     */
    public static Store getStore() {
        if (storeInstance == null) {
            synchronized (RedisStore.class) {
                if (storeInstance == null) {
                    storeInstance = new RedisStore();
                    ResourceBundle bundle = ResourceBundle.getBundle("KevinMqRedis");
                    host=bundle.getString("host");
                    port=Integer.parseInt(bundle.getString("port"));
                }
            }
        }
        return storeInstance;
    }

    /**
     * 把 body 数据保存到持久化文件中,该文件以日为单位创建
     */
    @Override
    public void save(Record record) {
        messageCache.add(record);
        if (messageCache.size() >= MESSAGE_CACHE_MAXSIZE) {
            synchronized (RedisStore.class) {
                if (messageCache.size() >= MESSAGE_CACHE_MAXSIZE) {
                    store(messageCache);
                }
            }
        }
    }

    /**
     * 将缓存中的消息全部持久化保存
     */
    @Override
    public void flush() {
        store(messageCache);
    }

    /**
     * 持久化保存文件
     */
    private synchronized <T> void store(List<T> datalist) {
        List<T> list = new ArrayList<>(datalist);
        //准备文件

        Object[] array = list.toArray();
        String[] data = new String[array.length];
        for (int i = 0; i < array.length; i++) {
            data[i] = array[i].toString();
        }

        JedisPooled jedisPooled = new JedisPooled(host,port);
        jedisPooled.rpush(dateFormat.format(new Date()), data);

        jedisPooled.close();
        messageCache.clear();
    }
}
