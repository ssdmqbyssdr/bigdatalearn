package hadoopexample.MysqlOption;

import org.junit.Before;

import  java.sql.*;

public class mysql_test {

    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://106.75.120.183:3306/mpc_test?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

    static final String USER = "root";
    static final String PASS = "YUBINucloud123";

    @Before
    public void init() throws ClassNotFoundException, SQLException {
        Class.forName(JDBC_DRIVER);
        Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
        Statement smt = conn.createStatement();
    }
}


