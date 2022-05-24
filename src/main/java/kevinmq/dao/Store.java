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
public interface Store {
    /**
     * 把 body 数据保存到持久化文件中,该文件以日为单位创建
     * @param record 要存储的记录
     */
    void save(Record record);

    /**
     * 将缓存中的消息全部持久化保存
     */
    void flush();

}
