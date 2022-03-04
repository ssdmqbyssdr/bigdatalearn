package hadoopexample.tool_yubin;


import java.io.*;
import java.util.Enumeration;
import java.util.Properties;

public class GetProperties {

    public GetProperties() {
    }

    //�����ļ�·���͵�ַ�õ���Ӧ��value
    public  String GetProperties(String filePath,String key){
        Properties prop = new Properties();
        try{
            InputStream input = new BufferedInputStream(new FileInputStream(filePath));
            prop.load(input);
            String value =prop.getProperty(key);
            return  value;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public  GetProperties(String filePath) throws IOException{
        Properties prop = new Properties();
        InputStream input = new BufferedInputStream(new FileInputStream(filePath));
        prop.load(input);
        Enumeration enumer =prop.propertyNames();  //�õ������ļ�������

        while(enumer.hasMoreElements()){
            String strKey = (String) enumer.nextElement();
            String strValue = prop.getProperty(strKey);
        }
    }

    public static void WriteProperties(String filePath,String key,String pValue) throws IOException {
        Properties prop = new Properties();
        InputStream input = new FileInputStream(filePath);
        //���������ж�ȡ�����б�
        prop.load(input);
        //���� Hashtable �ķ��� put��ʹ�� getProperty �����ṩ�����ԡ�
        //ǿ��Ҫ��Ϊ���Եļ���ֵʹ���ַ���������ֵ�� Hashtable ���� put �Ľ����
        OutputStream output = new FileOutputStream(filePath);
        prop.setProperty(key,pValue);
        //���ʺ�ʹ�� load �������ص� Properties ���еĸ�ʽ��
        //���� Properties ���е������б�����Ԫ�ضԣ�д�������
        prop.store(output,key);
    }

    public static void main(String[] args) throws  IOException{
//        WriteProperties("src/main/java/yubin_test/usdp.properties","long","212");
//        GetAllProperties("src/main/java/yubin_test/usdp.properties");
    }

}
