package hadoopexample.HiveOption;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Hive_test {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://udp01:10000/";
    private static String user = "hadoop";
    private static String password = "hadoop";

    private static Connection conn = null;
    private static Statement statement = null;
    private static ResultSet rs = null;

    public static void main(String[] args) throws Exception {
        showDatabases();
        createDatabase("databaseName");
        createTable("databaseName","create sql");

    }
    @Before
    public void init() throws Exception {
        Class.forName(driverName);
        conn = DriverManager.getConnection(url, user, password);
        statement = conn.createStatement();
    }

    //�鿴��
    @Test
    public static  void showDatabases() throws Exception {
        String sql = "show databases";
        System.out.println("Running: " + sql + "\n");
        rs = statement.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    //������
    @Test
    public static void createDatabase(String databaseNname) throws Exception {
        String sql = "create database " + databaseNname;
        System.out.println("Running: " + sql);
        statement.executeQuery(sql);
    }

    //ɾ����
    @Test
    public static  void dropDatabase(String databaseNname) throws Exception {
        String sql = "drop database if exists" + databaseNname;
        System.out.println("Running: " + sql);
        statement.execute(sql);
    }

    //������
    @Test
    public static void createTable(String databaseName,String createTableSql) throws Exception {
//        String createTableSql = "create table xueyuan_hive_01(id int ,name String) row format delimited fields terminated by ',';";
        String sql = "use" + databaseName;
        System.out.println("Running: " + sql);
        statement.execute(sql);
        System.out.println("Running: " + createTableSql);
        statement.execute(createTableSql);
        System.out.println("����" + createTableSql + "�ɹ�");
    }

    //��ѯ��
    @Test
    public static void selectData(String databaseName) throws Exception {
        String sql = "select * from " + databaseName;
        System.out.println("Running: " + sql);
        rs = statement.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getInt(1) + "\t" + rs.getString(2));
        }
    }


    //���ر�
    @Test
    public static void loadData() throws Exception {
        String filePath = "/usr/tmp/student";
        String sql = "load data local inpath '" + filePath + "' overwrite into table t2";
        System.out.println("Running: " + sql);
        statement.execute(sql);
    }

    //ɾ����
    @Test
    public static void drop(String tableName) throws Exception {
        String dropSQL = "drop table " + tableName;
        boolean bool = statement.execute(dropSQL);
        System.out.println("ɾ�����Ƿ�ɹ�:" + bool);
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

