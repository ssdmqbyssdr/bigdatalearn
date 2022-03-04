package hadoopexample.SparkOption;
import org.apache.spark.sql.SparkSession;

import java.text.ParseException;



public class SparkTest {
    public static void main(String[] args) throws ParseException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .master("yarn-cluster")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();
        spark.sql("show databases").show();
        spark.sql("use yubin_test;create table test_table_1207");
        spark.sql("use test_database_1207;show tables;").show();
    }
}

