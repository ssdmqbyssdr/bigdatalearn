package hadoopexample.HDFSOption;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.After;

import java.io.IOException;
import java.net.URI;


public class hdfs_read {
//    public static Logger logger=Logger.getLogger(hdfs_read.class);
    public final static String uri="hdfs://bms-ucloud04:8020";
    public final static String user_name="root";
    private Path path = null;
    private FileSystem fileSystem = null;



    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(uri),configuration,user_name);
        String option = System.getProperty("option");
        if (option == "mkdir"){
            String mkdir_path =  System.getProperty("path");
            mkdir_hdfs(fileSystem,mkdir_path);
        }
        else if (option == "upload"){
            String local_path =  System.getProperty("local_path");
            String hdfs_path =  System.getProperty("hdfs_path");
            upload_file(fileSystem,local_path,hdfs_path);
        }
        else if (option == "download"){
            String local_path =  System.getProperty("local_path");
            String hdfs_path =  System.getProperty("hdfs_path");
            download_file(fileSystem,local_path,hdfs_path);
        }

    }

    @After
    public void after() throws Exception{
        fileSystem.close();
    }

    private static void mkdir_hdfs(FileSystem  fileSystem, String mkdir_path) throws IOException {
        boolean b = fileSystem.mkdirs(new Path(mkdir_path));
        if (b){
            System.out.println(mkdir_path + "�����ɹ���");
        }else {
            System.out.println(mkdir_path + "�����Ѵ��ڣ�����ʧ�ܣ�");
        }
    }


    private static void upload_file(FileSystem  fileSystem, String local_path,String hdfs_path) throws IOException {
        fileSystem.copyFromLocalFile(
                new Path(local_path),
                new Path(hdfs_path));
    }


    private static void download_file(FileSystem  fileSystem, String local_path,String hdfs_path) throws IOException {
        fileSystem.copyToLocalFile(
                new Path(hdfs_path),
                new Path(local_path)
                );
    }

}
