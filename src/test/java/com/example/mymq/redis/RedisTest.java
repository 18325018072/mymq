package com.example.mymq.redis;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPooled;

import java.io.FileReader;
import java.io.InputStream;
import java.util.*;
import java.util.function.Function;

/**
 * @author Kevin2
 */
public class RedisTest {
    @SneakyThrows
    public static void main(String[] args) {
//        Properties properties = new Properties();
        InputStream stream = RedisTest.class.getClassLoader().getResourceAsStream("KevinMqRedis.properties");
//        properties.load(stream);

        PropertyResourceBundle bundle = new PropertyResourceBundle(stream);
        JedisPooled pooled = new JedisPooled(bundle.getString("host"), Integer.parseInt(bundle.getString("port")));
//        JedisPooled pooled = new JedisPooled(properties.getProperty("host"), Integer.parseInt(properties.getProperty("port")));
        System.out.println(pooled.get("6"));
        pooled.set("6","me");
        System.out.println(pooled.get("5") + pooled.get("6"));
        pooled.close();
//        JedisPool jedisPool = new JedisPool(properties.getProperty("host"),Integer.parseInt(properties.getProperty("port")));
//        try(Jedis jedis = jedisPool.getResource()) {
//            System.out.println(jedis.get("5"));
//            jedis.set("5","fuck");
//            System.out.println(jedis.get("5"));
//        }
//        jedisPool.close();
    }


    @Test
    public void ter(){
        List<Integer> datalist=new ArrayList<>(5);
        datalist.add(55);
        datalist.add(56);
        datalist.add(57);
        Object[] array = datalist.toArray();
        String[] data = new String[array.length];
        for (int i = 0; i < array.length; i++) {
            data[i]=array[i].toString();
        }


        ResourceBundle bundle = ResourceBundle.getBundle("KevinMqRedis");
        JedisPooled jedisPooled = new JedisPooled(bundle.getString("host"), Integer.parseInt(bundle.getString("port")));
        jedisPooled.rpush("de",data);
//        for (long i = 0; i < jedisPooled.llen("de"); i++) {
//            System.out.println(jedisPooled.rpop("de"));
//        }
        jedisPooled.close();
    }

}
