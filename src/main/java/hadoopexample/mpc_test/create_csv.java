package hadoopexample.mpc_test;


import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class create_csv {
    /**
     * ���������λ��
     */
    public static int Random_int() {
        int i = (int) (Math.random() * 900) + 100;
        int j = new Random().nextInt(900) + 100;
        return i;
    }

    @Test
    public void create_csv() {
        int a = Random_int();
        int b = Random_int();
        int c = Random_int();
        for (int i = 0; i <= 1000; i++) {
            System.out.println(a + "," + b + "," + c);
        }

    }
    @Test
    public  void main()  {
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter("mpc_data.csv"));
            for (int i = 0; i <= 10000; i++) {
                int a = Random_int();
                int b = Random_int();
                int c = Random_int();
                String str = "";
                str = str+a+","+b+","+c+"/n";
                out.write(str);
            }
            out.close();
                System.out.println("�ļ������ɹ���");
            } catch (IOException e) {
            }
        }
    }


