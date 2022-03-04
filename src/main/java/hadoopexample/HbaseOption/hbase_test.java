package hadoopexample.HbaseOption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class hbase_test {
    public static void main(String[] args){
        //�����࣬ͨ������zookeeper����hbase�����������������Ҫ��ip��ַ���Լ�����windowsӳ���ϵ
        Configuration conf=new Configuration();
        conf.set("hbase.zookeeper.quorum","master");
        try {
            //����hbase
            Connection con=ConnectionFactory.createConnection(conf);
            //��ȡt01��
            Table table=con.getTable(TableName.valueOf("t01"));
            //��t01�������һ������
            Put put=new Put(Bytes.toBytes("001"));
            put.addColumn(Bytes.toBytes("cf01"),Bytes.toBytes("age"),Bytes.toBytes("100"));
            table.put(put);
            //��ѯt01��������������
            Scan scan=new Scan();
            ResultScanner result=table.getScanner(scan);//�õ��������ݼ�
            Result str = null;
            while((str=result.next())!=null){
                List<Cell> cells = str.listCells();//��ÿ�����ݴ浽Cell������
                for (Cell c:cells) {
                    System.out.println(new String(CellUtil.cloneFamily(c))+new String(CellUtil.cloneQualifier(c))+new String(CellUtil.cloneValue(c)));
                }
            }
            //�õ�һ�����ݻ�һ������
            Get get=new Get("001".getBytes());
            get.addColumn("cf01".getBytes(),"name".getBytes());
            Result result1=table.get(get);//
            List<Cell> cells = result1.listCells();
            for (Cell c:cells) {
                System.out.println(new String(CellUtil.cloneFamily(c))+new String(CellUtil.cloneQualifier(c))+new String(CellUtil.cloneValue(c)));
            }
            //����1000������
            List<Put> list=new ArrayList<Put>();
            for (int i = 0; i < 1000; i++) {
                Put put1=new Put(Bytes.toBytes("00"+i));
                put1.addColumn("cf01".getBytes(),"name".getBytes(),("name_"+i).getBytes());
                list.add(put1);
            }
            table.put(list);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
