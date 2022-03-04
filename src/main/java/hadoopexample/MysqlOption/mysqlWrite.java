package hadoopexample.MysqlOption;


import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class mysqlWrite{
    public static Logger logger=Logger.getLogger(mysqlWrite.class);

    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("SparkMysql").setMaster("yarn-cluster"));
        SQLContext sqlContext = new SQLContext(sparkContext);
        //写入的数据内容
        JavaRDD<String> personData = sparkContext.parallelize(Arrays.asList("9999 SMITH CLERK 7902 1980-12-17 800 10 20", "9998 SMITH CLERK 7902 1980-12-17 800 10 20"));
        //数据库内容
        String url = "jdbc:mysql://10.0.0.7:33061/test_sql";
        String table = "EMP";

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "JJce2Lw82uB8r7R3JMo6");
        connectionProperties.put("driver", "com.mysql.jdbc.Driver");

        JavaRDD<Row> personsRDD = personData.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] splited = line.split(" ");
                return RowFactory.create(Integer.valueOf(splited[0]), splited[1], splited[2], Integer.valueOf(splited[3]), splited[4], Integer.valueOf(splited[5]), Integer.valueOf(splited[6]), Integer.valueOf(splited[7]));
            }
        });


        List<org.apache.spark.sql.types.StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("EMPNO", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("ENAME", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("JOB", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("MGR", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("HIREDATE", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("SAL", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("COMM", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("DEPTNO", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> personsDF = sqlContext.createDataFrame(personsRDD,structType);


        personsDF.write().mode("append").jdbc(url,table,connectionProperties);
        sparkContext.stop();
    }}

