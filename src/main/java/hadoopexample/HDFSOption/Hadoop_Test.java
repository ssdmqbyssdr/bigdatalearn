package hadoopexample.HDFSOption;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.*;
import java.net.URI;

public class Hadoop_Test {
    private Logger logger = LogManager.getLogger(this.getClass());
    private FileSystem fileSystem = null;
    public final static String user_name = "sf_wuling_c1_yubin";
    public final static String hdfs_uri = "hdfs://bms-ucloud04:8020";
    public final static String mkdir_path = "/hadoopexample";
    public final static String upload_local_file = "/Users/user/Documents/mpc_data.csv";
    public final static String upload_hdfs_file = "/hadoopexample/mpc_data.csv";
    public final static String copy_hdfs_file = "/hadoopexample/copy_mpc_data.csv";


    @Before
    public void before() throws Exception{
        Configuration configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(hdfs_uri),configuration,user_name);
    }

    @After
    public void after() throws Exception{
        if(fileSystem != null) {
            fileSystem.close();
        }
    }

    //�����ļ���
    @Test
    public  void mkdir(){
        try {
            boolean result = fileSystem.mkdirs(new Path(mkdir_path));
            logger.info("mkdir success",result);
        } catch (IllegalArgumentException |IOException e) {
            logger.error("create error",e);
        }
    }

    //�ϴ��ļ�
    @Test
    public void uploadFile() {
        InputStream input = null;
        OutputStream output = null;
        try {
            input = new FileInputStream(upload_local_file);
            output = fileSystem.create(new Path(upload_hdfs_file));
            fileSystem.copyFromLocalFile(new Path(upload_local_file),new Path(upload_hdfs_file));
            //IOUtils.copyBytes(input, output, 4096, true);
            logger.error("�ϴ��ļ��ɹ�");
        } catch (IllegalArgumentException | IOException e) {
            logger.error("�ϴ��ļ�����", e);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                }
            }
            if (output != null) {
                try {
                    output.close();
                } catch (IOException e) {
                }
            }
        }
    }

    /***
    @Test
    public void copyFile(){
        try{
            boolean result = fileSystem.copyToLocalFile("ture",upload_hdfs_file,copy_hdfs_file);
        }
    }***/

    //�����ļ�
    @Test
    public void downFile() {
        String fileName = "hadoop.txt";
        InputStream input = null;
        OutputStream output = null;
        try {
            input = fileSystem.open(new Path("/test/" + fileName));
            output = new FileOutputStream("F:\\down\\" + fileName);
            IOUtils.copyBytes(input, output, 4096, true);
            logger.error("�����ļ��ɹ�");
        } catch (IllegalArgumentException | IOException e) {
            logger.error("�����ļ�����", e);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                }
            }
            if (output != null) {
                try {
                    output.close();
                } catch (IOException e) {
                }
            }
        }

    }

    //ɾ���ļ�
    @Test
    public void deleteFile() {
        String fileName = "hadoop.txt";
        try {
            boolean result = fileSystem.delete(new Path("/test/" + fileName), true);
            logger.info("ɾ���ļ������{}", result);
        } catch (IllegalArgumentException | IOException e) {
            logger.error("ɾ���ļ�����", e);
        }
    }

    //�����ļ�
    @Test
    public void listFiles() {
        try {
            FileStatus[] statuses = fileSystem.listStatus(new Path("/"));
            for (FileStatus file : statuses) {
                logger.info("ɨ�赽�ļ���Ŀ¼�����ƣ�{}���Ƿ�Ϊ�ļ���{}", file.getPath().getName(), file.isFile());
                System.out.println(logger);
            }
        } catch (IllegalArgumentException | IOException e) {
            logger.error("�����ļ�����", e);
        }
    }

    @Test
    public void printFiles(){
        try {
            RemoteIterator<LocatedFileStatus> statuses = fileSystem.listFiles(new Path("/"),true);
            System.out.println(statuses.getClass().toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}