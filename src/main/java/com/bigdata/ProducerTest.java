package com.bigdata;

/**
 * Created with IntelliJ IDEA.
 * Author: kang.liu
 * Date  : 2018/6/5 17:41
 * Description:
 */

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

public class ProducerTest {
    public static void main(String[] args) {
        producer_test1(args);

//        producer_test2();
    }

    /**
     * 参数设置备注：
     * 1）bootstrap.servers --设置生产者需要连接的kafka地址
     * 2）acks --回令类型
     * 3）retries --重试次数
     * 4）batch.size --批量提交大小
     * 5）linger.ms --提交延迟等待时间（等待时间内可以追加提交）
     * 6）buffer.memory --缓存大小
     * 7）key.serializer|value.serializer --序列化方法
     * <p>
     * <p>
     * 1、acks回令。如果必须等待回令，那么设置acks为all；否则，设置为-1；等待回令会有性能损耗。
     * 2、生产者在发送消息的过程中，会自己默认批量提交。所以，如果单条指令的发送请求，记得发送完后flush才能生效。
     * <p>
     * 3、SimplePartitioner2.java为kafaka分区，可选项。
     */
    private static void producer_test2() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.56.3:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("kafakatopic", Integer.toString(i), Integer.toString(i)));
        }
        producer.close();
    }

    private static void producer_test1(String[] args) {
        String arg0 = args != null && args.length > 0 ? args[0] : "10";
        long events = Long.parseLong(arg0);
        Random rnd = new Random();

        //    /opt/kafka_2.12-1.1.0/bin/kafka-console-producer.sh --broker-list 192.178.0.111:9092 --sync --topic kafkatopic
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.56.3:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 配置partitionner选择策略，可选配置
        props.put("partitioner.class", "com.bigdata.producer.SimplePartitioner2");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (long nEvents = 0; nEvents < events; nEvents++) {
            long runtime = System.currentTimeMillis();
            String ip = "192.168.56.3." + rnd.nextInt(255);
            String msg = runtime + ",www.example.com," + ip;
            ProducerRecord<String, String> data = new ProducerRecord<String, String>("bigdata_02", ip, msg);
            Future<RecordMetadata> send = producer.send(data, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    } else {
                        System.out.println("The offset of the record we just sent is: " + metadata.offset());
                    }
                }
            });
        }
        producer.close();
    }
}