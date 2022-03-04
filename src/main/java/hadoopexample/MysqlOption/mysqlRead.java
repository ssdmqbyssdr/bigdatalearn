package hadoopexample.MysqlOption;


import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

import java.util.Properties;

public class mysqlRead{
    public static Logger logger=Logger.getLogger(mysqlRead.class);

    public static void main(String[] args){
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf()
                .setMaster("yarn-cluster")
                .setAppName("mysql read")
        );
        SQLContext sqlContext = new SQLContext(sparkContext);
        //read mysql
        readMySQL(sqlContext);

        // stop sparkcontext
        sparkContext.stop();
    }

    private static void readMySQL(SQLContext sqlContext){
        //jdbc.url
        String url = "jdbc:mysql://10.0.0.7:33061/test_sql";
        String table = "EMP";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","JJce2Lw82uB8r7R3JMo6");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");

        Dataset jdbcDF = sqlContext.read().jdbc(url,table,connectionProperties).select("*");
        jdbcDF.show();
    }
}

