package hadoopexample.KafkaOption;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.spark_project.guava.collect.Lists;

import java.util.*;

import static jodd.util.ThreadUtil.sleep;

public class MyKafka {

    private final KafkaProducer<String, String> producer;

    private final KafkaConsumer<String, String> consumer;

    public final static String TOPIC = "o-log-task-monitor";

    public MyKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "udp01:9092,udp02:9092,udp03:9092");//xxx服务器ip
        props.put("acks", "all");//所有follower都响应了才认为消息提交成功，即"committed"
        props.put("retries", 0);//retries = MAX 无限重试，直到你意识到出现了问题:)
        props.put("batch.size", 16384);//producer将试图批处理消息记录，以减少请求次数.默认的批量处理消息字节数
        //batch.size当批量的数据大小达到设定值后，就会立即发送，不顾下面的linger.ms
        props.put("linger.ms", 1);//延迟1ms发送，这项设置将通过增加小的延迟来完成--即，不是立即发送一条记录，producer将会等待给定的延迟时间以允许其他消息记录发送，这些消息记录可以批量处理
        props.put("buffer.memory", 33554432);//producer可以用来缓存数据的内存大小。
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", UUID.randomUUID().toString());


        producer = new KafkaProducer<String, String>(props);
        consumer = new KafkaConsumer<String, String>(props);

    }

    //生产
    @Test
    public void produce() {
        int messageNo = 1;
        final int COUNT = 5;

        while (messageNo < COUNT) {
//            String data = String.format("{\"age\":1,\"name\":\"chenlan\",\"enheng\":\"ee\",\"name1\":\"chenlan\"}");
            String data = String.format("{\"age\":1,\"name\":\"chenlan\",\"enheng\":\"ee\"}");
            try {
                producer.send(new ProducerRecord<String, String>(TOPIC, data));
            } catch (Exception e) {
                e.printStackTrace();
            }
            messageNo++;
        }
        producer.close();
    }

    //消费
    @Test
    public void consumer() {
        List opt =  Lists.newArrayList(1, 2);
        consumer.beginningOffsets(opt);
        consumer.subscribe(Arrays.asList(TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("-----------------");
                System.out.printf("offset = %d, value = %s", record.offset(), record.value());
                System.out.println();
            }
        }
    }

    //创建topic

    //删除topic
    //获取topic


}
