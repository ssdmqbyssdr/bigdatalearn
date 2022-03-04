//package yubin_test.umeta;
//
//import cn.hutool.core.date.DateTime;
//import cn.hutool.core.date.DateUtil;
//import cn.hutool.core.lang.Singleton;
//import com.fasterxml.jackson.annotation.JsonProperty;
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//import lombok.AllArgsConstructor;
//import lombok.Data;
//import lombok.NoArgsConstructor;
//import org.junit.Test;
//
//
//import java.util.Properties;
//import java.util.concurrent.ExecutionException;
//
///**
// * @author lan.chen
// * @version 1.0
// * @description: TODO
// * @date 2021-12-08 2:38 PM
// */
//
//
//
//public class NariLogGenerator {
//    @Test
//    public static void main(String[] args) throws JsonProcessingException, ExecutionException, InterruptedException {
//
//        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "udp01:9092,udp02:9092,udp03:9092");
//        properties.put("topic", "o-log-task-monitor-2");
////        UMetaKafkaSender sender = new UMetaKafkaSender(properties);
//
//        for (int i = 0; i < 1000; i++) {
//            NariLogLineageVo vo1 = new NariLogLineageVo(
//                    String.valueOf(i),
//                    "test1",
//                    "Spark",
//                    "t1",
//                    "0",
//                    "mysql",
//                    "10000",
//                    DateTime.of(DateUtil.parse("2021-12-01 22:35:19").getTime()+i).toString());
//
//            NariLogLineageVo vo2 = new NariLogLineageVo(
//                    String.valueOf(i),
//                    "test1",
//                    "Spark",
//                    "usdp_test",
//                    "1",
//                    "kafka",
//                    "9999",
//                    DateTime.of(DateUtil.parse("2021-12-01 22:35:19").getTime()+i).toString());
//
//            NariLogLineageVo vo3 = new NariLogLineageVo(
//                    String.valueOf(i),
//                    "test1",
//                    "Spark",
//                    "test5",
//                    "1",
//                    "kafka",
//                    "99999",
//                    DateTime.of(DateUtil.parse("2021-12-01 22:35:19").getTime()+i).toString());
//            System.out.println(Singleton.get(ObjectMapper.class).writeValueAsString(vo1));
//            System.out.println(Singleton.get(ObjectMapper.class).writeValueAsString(vo2));
//            System.out.println(Singleton.get(ObjectMapper.class).writeValueAsString(vo3));
////            sender.send(Singleton.get(ObjectMapper.class).writeValueAsString(vo1));
////            sender.send(Singleton.get(ObjectMapper.class).writeValueAsString(vo2));
////            sender.send(Singleton.get(ObjectMapper.class).writeValueAsString(vo3)).get();
//        }
//    }
//
//
//    @Data
//    @NoArgsConstructor
//    @AllArgsConstructor
//    public static class NariLogLineageVo {
//        @JsonProperty("applicationId")
//        private String applicationId;
//
//        @JsonProperty("applicationName")
//        private String applicationName ;
//
//        @JsonProperty("applicationType")
//        private String applicationType;
//
//        @JsonProperty("tableName")
//        private String tableName;
//
//        @JsonProperty("operaType")
//        private String operaType;
//
//        @JsonProperty("tableType")
//        private String tableType;
//
//        @JsonProperty("tableCount")
//        private String tableCount;
//
//        @JsonProperty("computerTime")
//        private String computerTime;
//    }
//}
//
