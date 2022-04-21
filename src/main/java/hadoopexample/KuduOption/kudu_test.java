package hadoopexample.KuduOption;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.junit.Before;
import org.junit.Test;


import java.util.LinkedList;
import java.util.List;

public class kudu_test {
    //����KuduClient�ͻ��˶���
    private static KuduClient kuduClient;
    //�������
    private static String tableName = "yubin_test.xueyuan_kudu_01";
    /**
     * ��ʼ������
     */
    @Before
    public void init() {
    //ָ��master��ַ
        String masterAddress = "nari-test01,nari-test02,nari-test03,nari-test04,nari-test05";
    //����kudu�����ݿ�����
        kuduClient = new KuduClient.KuduClientBuilder(masterAddress).defaultSocketReadTimeoutMs(6000).build();
    }
    //������schema���ֶ���Ϣ
    //�ֶ����� �������� �Ƿ�Ϊ���� ����
    public ColumnSchema newColumn(String name, Type type, boolean isKey,String comment) {
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        column.key(isKey);
        column.comment(comment);
        return column.build();
    }

    public ColumnSchema newColumn(String name, Type type, boolean isKey,String comment,String defalutvalue) {
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        column.key(isKey);
        column.comment(comment);
        column.defaultValue(defalutvalue);
        return column.build();
    }


    /**
     * ʹ��junit���в���
     * <p>
     * ������
     *
     * @throws KuduException
     */
    @Test
    public void createTable() throws KuduException {
//���ñ��schema
        List<ColumnSchema> columns = new LinkedList<ColumnSchema>();
//        columns.add(newColumn("CompanyId", Type.INT32, true,"��˾ID"));
//        columns.add(newColumn("WorkId", Type.INT32, false,"����"));
//        columns.add(newColumn("Name", Type.STRING, false,"����"));
//        columns.add(newColumn("Gender", Type.STRING, false,"��֪����ɶ"));
//        columns.add(newColumn("Photo", Type.STRING, false,"��Ƭ"));
        columns.add(newColumn("age",Type.INT32,true,"age"));
        columns.add(newColumn("name",Type.STRING,true,"name"));
        columns.add(newColumn("enheng",Type.STRING,true,"enen"));
        Schema schema = new Schema(columns);
        //������ʱ�ṩ������ѡ��
        CreateTableOptions tableOptions = new CreateTableOptions();
        //���ñ�ĸ����ͷ�������
        LinkedList<String> list = new LinkedList<String>();
        list.add("age");
        //���ñ�����
        tableOptions.setNumReplicas(1);
        //����range����
        //tableOptions.setRangePartitionColumns(list);
        //����hash�����ͷ���������
        tableOptions.addHashPartitions(list, 3);
        try {
            kuduClient.createTable(tableName,schema,tableOptions);
        } catch (Exception e) {
            e.printStackTrace();
        }
        kuduClient.close();
        }

        //ɾ����
    @Test
    public void DeleteTable() throws KuduException {
        // ����kudu�����ݿ�����
        kuduClient.deleteTable(tableName);
        kuduClient.close();
    }

    //�����ֶ�
    @Test
    public void add_column() throws KuduException {
        AlterTableOptions ato = new AlterTableOptions();
        ato.addColumn(newColumn("addColumn6", Type.STRING, false,"�����ĵ�1���ֶ�","12"));
        kuduClient.alterTable(tableName,ato);
        kuduClient.close();
    }

    //��������
    @Test
    public void rename_table() throws KuduException{
        AlterTableOptions ato = new AlterTableOptions();
        ato.renameTable("person_new");
        kuduClient.alterTable(tableName, ato);
        kuduClient.close();
    }

    // ɾ���ֶ�
    @Test
    public void delete_column() throws KuduException {
        AlterTableOptions ato = new AlterTableOptions();
//        ato.addColumn(newColumn("addColumn", Type.STRING, false, "�������ֶ�", "12"));
        ato.dropColumn("newColumn");
        kuduClient.alterTable(tableName,ato);
        kuduClient.close();
    }

    //�������ֶ�
    @Test
    public  void rename_column() throws KuduException {
        AlterTableOptions ato = new AlterTableOptions();
//        ato.addColumn(newColumn("addColumn", Type.STRING, false, "�������ֶ�", "12"));
        ato.renameColumn("addColumn4","newColumn");
        kuduClient.alterTable(tableName, ato);
        kuduClient.close();
    }

    //�޸��ֶ�����
    @Test
    public  void change_column_comment() throws KuduException {
        AlterTableOptions ato = new AlterTableOptions();
    //        ato.addColumn(newColumn("addColumn", Type.STRING, false, "�������ֶ�", "12"));
        ato.changeComment("Gender","���ǲ�֪����ɶ");
        kuduClient.alterTable(tableName, ato);
        kuduClient.close();
    }

    //�޸�ѹ���㷨
    @Test
    public  void change_Compression_Algorithm() throws KuduException {
        AlterTableOptions ato = new AlterTableOptions();
    //        ato.addColumn(newColumn("addColumn", Type.STRING, false, "�������ֶ�", "12"));
        ato.changeCompressionAlgorithm("CompanyId", ColumnSchema.CompressionAlgorithm.SNAPPY);
        kuduClient.alterTable(tableName, ato);
        kuduClient.close();
    }

    @Test
    public  void change_tableOptions() throws KuduException {


    }

    @Test
    public  void show_tables() throws KuduException {
        System.out.println(kuduClient.getTablesList().toString());

    }
}


