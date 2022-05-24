package com.example.mymq.redis;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPooled;

import java.io.FileReader;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Kevin2
 */
public class RedisTest {
    @SneakyThrows
    public static void main(String[] args) {
        Properties properties = new Properties();
        InputStream stream = RedisTest.class.getClassLoader().getResourceAsStream("KevinMqRedis.properties");
        properties.load(stream);

        JedisPooled pooled = new JedisPooled(properties.getProperty("host"), Integer.parseInt(properties.getProperty("port")));
        System.out.println(pooled.get("6"));
        pooled.set("6","you");
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



}
