package hadoopexample.HiveOption;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class HiveWriter {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://usdp-clustera-01:10000/";
    private static String user = "hadoop";
    private static String password = "hadoop";

    private static Connection conn = null;
    private static Statement statement = null;
    private static ResultSet rs = null;

    public static void main(String[] args) throws Exception {
        createHiveDatabase(1);

    }
    @Before
    public void init() throws Exception {
        Class.forName(driverName);
        conn = DriverManager.getConnection(url, user, password);
        statement = conn.createStatement();
    }


    @Test
    public static void createHiveDatabase(int databasesCount) throws Exception {

        String sql = "create database " + databasesCount;
        statement.executeQuery(sql);

        for(int i =1;i <=databasesCount;i++){
            String create_database_sql ="create database database"+i+";";
            statement.executeQuery(create_database_sql);
            for (int j =1; j<=50; j++){
                String create_table_sql="create table  database" + i + "." + "table" + j + "(`column1` string);";
                statement.execute(create_table_sql);
                for (int x =2;x<=50 ;x++ ){
                    String add_column_sql ="alter table database" + i + "." + "table" + j +" add columns(columns"+ x +" varchar(32);";
                    statement.execute(add_column_sql);

                    System.out.println(add_column_sql);
                }
            }
        }
    }

    @After
    public void destory() throws Exception {
        if (rs != null) {
            rs.close();
        }
        if (statement != null) {
            statement.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
