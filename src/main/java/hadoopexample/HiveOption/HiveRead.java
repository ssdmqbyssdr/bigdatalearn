package hadoopexample.HiveOption;

import org.apache.spark.sql.SparkSession;

import java.text.ParseException;

public class HiveRead {
    public static void main(String[] args) throws ParseException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .master("yarn-cluster")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();
        spark.sql("show databases").show();
    }

    public static class HiveWrite {

        public static void main(String[] args) throws ParseException {
            SparkSession spark = SparkSession
                    .builder()
                    .appName("Java Spark Hive Example")
                    .master("yarn-cluster")
                    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                    .enableHiveSupport()
                    .getOrCreate();
            spark.sql("create table default.sqoop3(EMPNO int,ENAME string, JOB string,MGR int,HIREDATE DATE,SAL int,COMM int,DEPTNO int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'").show();
            spark.sql("insert into table default.sqoop2 values(7369,'SMITH','CLERK',7902,'1980-12-17',800,10,20)");
        }
    }
}



