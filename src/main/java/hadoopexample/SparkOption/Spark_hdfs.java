package hadoopexample.SparkOption;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class Spark_hdfs {
    public final static String uri="hdfs://bms-ucloud04:8020";
    public final static String user_name="root";
    private FileSystem fileSystem = null;
    private Path  path = null;
    public static void main(String[] argvs){

    }

    @Before
    public void before() throws Exception{
        Configuration configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(uri),configuration,user_name);
        path = new Path(uri);

    }

    @After
    public void after() throws Exception{
        fileSystem.close();
    }

    @Test
    public void read_path() throws Exception{
        FileStatus status[] = fileSystem.listStatus(path);

    }

    @Test
    public  void mkdir_file() throws IOException{
        try {
            fileSystem.create(new Path("/hadoopexample"));
        }catch (IllegalArgumentException | IOException e) {
            System.out.println(e);
        }

    }

    @Test
    public void get_file_csv() throws IOException {
        InputStream in =null;
        try {
            in = fileSystem.open(new Path("/hadoopexample/test2.csv"));
            IOUtils.copyBytes(in,System.out,4096,true);
        }catch (IllegalArgumentException | IOException e) {
            System.out.println(e);
        }

    }
}
