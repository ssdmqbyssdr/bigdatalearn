package databasesexample.mysqloption;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlTest {

    public static String mysqlDriver = "com.mysql.jdbc.driver";
    public static String username="root";
    public static String password="1qaz!QAZ";
    public static String url = "jdbc:mysql://nari-test01";
    public static Connection conn;

    static {
        try {
            conn = DriverManager.getConnection(url,username,password);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void createMysqlDatabase(int databasesCount) throws SQLException {

      Connection conn =  DriverManager.getConnection(url,username,password);
//      int i = databasesCount;
        for(int i =21;i <=databasesCount;i++){
            String create_database_sql ="create database database"+i+";";
            PreparedStatement statement = conn.prepareStatement(create_database_sql);
            statement.execute();
            for (int j =1; j<=50; j++){
                String create_table_sql = "create table" + " database" + i + "." + "table" + j + "(`column1` varchar(32))";                PreparedStatement statement1 = conn.prepareStatement(create_table_sql);
                statement1.execute();
                System.out.println(create_table_sql);
                for (int x =2;x<=50 ;x++ ){
                    String add_column_sql ="alter table " +  "database" + i + "." + "table" + j +" add column"+ x+" varchar(32);";
                    PreparedStatement statement2 = conn.prepareStatement(add_column_sql);
                    statement2.execute();
//                    System.out.println(add_column_sql);
                }

            }


        }
//        PreparedStatement statement = conn.prepareStatement(sql);
    }

    public static void main(String[] args) throws SQLException {
        createMysqlDatabase(50);
    }


}
