package hadoopexample.FlumeOption;


import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.SourceRunner;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.avro.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class MysqlSource extends AbstractSource implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(MysqlSource.class);
    private Connection connect;
    private Statement state;
    private String url;
    private String user;
    private String password;
    private String tableName;

    @Override
    public  synchronized void  stop(){
        super.stop();
    }

    @Override
    public  synchronized void start(){
        super.start();
        try {
            connect = DriverManager.getConnection(url,user,password);
            //jdbc:mysql//url/
            state = connect.createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public Status process() throws EventDeliveryException {
        return null;
    }

        @Override
    public void configure(Context context) {

    }
}
