package hadoopexample.FlumeOption;

import com.google.common.base.Preconditions;
import org.apache.flume.*;
import org.apache.flume.sink.AbstractSink;

import org.apache.flume.conf.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public  class MysqlSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(MysqlSink.class);
    private Connection connect;
    private Statement state;
    private String columnName;
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

    @Override
    public Status process() throws EventDeliveryException {
        //获取当前Sink绑定的Channel
        Channel ch = getChannel();
        //获取事务
        Transaction transaction = ch.getTransaction();
        //声明事件
        Event event = null;
        //开启事务
        transaction.begin();

        //读取Channel中的事件，直到读取到事件结束循环
        while(true){
            event = ch.take();
            if (event != null ){
                break;
            }

        }
        try {
            //处理事件
            String rawbody = new String(event.getBody());
            String body = rawbody.split("\t")[2];
            if (body.split(",").length == columnName.split(",").length){
                String sql = "insert into " + tableName + "(" + columnName + ") values (" + body + ")";
                state.executeUpdate(sql);

                //事物提交
                transaction.commit();
                return Status.READY;
            }else {
                //遇到异常，事务回滚
                transaction.rollback();
                return null;
            }
        } catch (Throwable throwables) {
            transaction.rollback();
            if (throwables instanceof Error){
                throw (Error) throwables;
            }else {
                throw new EventDeliveryException(throwables);
            }
        } finally {
            //关闭事务
            transaction.close();
        }
    }

    @Override
    public void configure(Context context) {
        //读取配置文件内容
        columnName = context.getString("column_name");
        Preconditions.checkNotNull(columnName,"column_name must be set");
        url = context.getString("url");
        user = context.getString("user");
        password = context.getString("password");
        tableName = context.getString("tableName");

    }
}
