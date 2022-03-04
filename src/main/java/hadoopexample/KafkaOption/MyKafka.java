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
        props.put("bootstrap.servers", "udp01:9092,udp02:9092,udp03:9092");//xxx������ip
        props.put("acks", "all");//����follower����Ӧ�˲���Ϊ��Ϣ�ύ�ɹ�����"committed"
        props.put("retries", 0);//retries = MAX �������ԣ�ֱ������ʶ������������:)
        props.put("batch.size", 16384);//producer����ͼ��������Ϣ��¼���Լ����������.Ĭ�ϵ�����������Ϣ�ֽ���
        //batch.size�����������ݴ�С�ﵽ�趨ֵ�󣬾ͻ��������ͣ����������linger.ms
        props.put("linger.ms", 1);//�ӳ�1ms���ͣ��������ý�ͨ������С���ӳ������--����������������һ����¼��producer����ȴ��������ӳ�ʱ��������������Ϣ��¼���ͣ���Щ��Ϣ��¼������������
        props.put("buffer.memory", 33554432);//producer���������������ݵ��ڴ��С��
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

    //����
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

    //����
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

    //����topic

    //ɾ��topic
    //��ȡtopic


}
