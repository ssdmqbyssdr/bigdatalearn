package hadoopexample.KafkaOption;


import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.After;
import org.junit.Test;
import scala.collection.JavaConversions;

import java.io.IOException;
import java.util.*;

public class TopicOption {
//    String zk =new GetProperties("");

    private static final ZkUtils ZK = ZkUtils.apply("usdp-clustera-01:2181,usdp-clustera-02:2181,usdp-clustera-03:2181",3000,3000, JaasUtils.isZkSecurityEnabled());

    public final static String TOPIC = "test";

    public static void main(String[] args) {
        createTopic();
//        changeTopicPartitions("2");
    }

    public TopicOption() throws IOException {
    }


    //获取topic list
    @Test
    public void GetTopicList(){
        List<String> TopicList= JavaConversions.seqAsJavaList(ZK.getAllTopics());
        System.out.println(TopicList);
    }

    //创建topic
    @Test
    public static void createTopic(){
        AdminUtils.createTopic(ZK,TOPIC,1,1,new Properties(),
                RackAwareMode.Enforced$.MODULE$);
    }

    @Test
    public static void changeTopicPartitions(int partitions){
        AdminUtils.addPartitions(ZK,TOPIC,partitions,"",true,RackAwareMode.Enforced$.MODULE$);
    }
    //查看topic详情

    @Test
    public void QueryTopic(){
        Properties prop = AdminUtils.fetchEntityConfig(ZK, ConfigType.Topic(),"__consumer_offsets");
        Iterator it = prop.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry entry = (Map.Entry)it.next();
            Object key =entry.getKey();
            Object value =entry.getValue();
            System.out.println(key + "=" + value);
        }
    }
    //删除topic
    @Test
    public void DeleteTopic(){
        AdminUtils.deleteTopic(ZK,TOPIC);
    }


    @Test
    public void TopicExists(){
        boolean exists = AdminUtils.topicExists(ZK,TOPIC);
        System.out.println(exists);
    }

    @After
    public void CloseZk(){
        ZK.close();
    }

}
