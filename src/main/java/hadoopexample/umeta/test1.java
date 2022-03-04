package hadoopexample.umeta;

import org.junit.Test;


public class test1 {
    @Test
    public void main() {
        String[] args ={"a","b","c"};
        test1(args);
    }

    public void test1(String[] args) {
        for (String numerial : args) {
            System.out.println(numerial);
        }
    }
}
