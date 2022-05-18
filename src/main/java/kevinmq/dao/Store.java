package kevinmq.dao;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

/**
 * Dao层的日志类
 * @author Kevin2
 * last updated:2022/5/16
 */
public class Store {
    /**
     * 刷盘对应文件，用于持久化数据
     */
    private File file;
    /**
     * 存储前，最多缓存多少条消息
     */
    private final int MESSAGE_CACHE_MAXSIZE =100;
    /**
     * 缓存容器
     */
    private ArrayList<Record> messageCache =new ArrayList<>();

    private static volatile Store storeInstance;

    /**
     * 饿汉单例模式创建 Store对象
     * @return Store对象
     */
    public static Store getStore() {
        if (storeInstance == null) {
            synchronized (storeInstance) {
                if (storeInstance == null) {
                    storeInstance = new Store();
                }
            }
        }
        return storeInstance;
    }

    /**
     * 把 body 数据保存到持久化文件中,该文件以日为单位创建
     */
    public void save(Record record) {
        messageCache.add(record);
        if (messageCache.size()>=MESSAGE_CACHE_MAXSIZE){
            store(messageCache);
        }
    }

    /**
     * 将缓存中的消息全部持久化保存
     */
    public void flush(){
        store(messageCache);
    }

    /**
     * 持久化保存文件
     */
    private void store(Object obj){
        //准备文件
        Date date = new Date();
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        file = new File("log/KevinMqLog_" + dateFormat.format(date) + ".txt");
        //写入文件
        try (FileOutputStream os = new FileOutputStream(file, true);
             ObjectOutputStream oos = new ObjectOutputStream(os)) {
            oos.writeObject(obj);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
