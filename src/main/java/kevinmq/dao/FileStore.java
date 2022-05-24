package kevinmq.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Dao层的日志类
 * @author Kevin2
 * last updated:2022/5/16
 */
public class FileStore implements Store{
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
    private List<Record> messageCache = Collections.synchronizedList(new ArrayList<>());

    protected static volatile Store storeInstance;

    /**
     * 饿汉单例模式创建 Store对象
     * @return Store对象
     */
    public static Store getStore() {
        if (storeInstance == null) {
            synchronized (Store.class) {
                if (storeInstance == null) {
                    storeInstance = new FileStore();
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
        if (messageCache.size()>=MESSAGE_CACHE_MAXSIZE){
            store(messageCache);
            messageCache.clear();
        }
    }

    /**
     * 将缓存中的消息全部持久化保存
     */
    @Override
    public void flush(){
        store(messageCache);
        messageCache.clear();
    }

    /**
     * 持久化保存文件
     */
    private <T> void store(List<T> datalist){
        List<T> list=new ArrayList<>(datalist);
        //准备文件
        Date date = new Date();
        DateFormat dateFormat = new SimpleDateFormat("yyyy_MM dd");
        File directory = new File("log");
        if (!directory.exists()) {
            System.out.println(directory.mkdirs());
        }
        file = new File("./log/KevinMqLog_" + dateFormat.format(date) + ".txt");
        //写入文件
        try (Writer writer = new FileWriter(file, true)) {
            for (T t : list) {
                IOUtils.write(date+t.toString()+"\n",writer);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
