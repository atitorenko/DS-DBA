package main.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Main {
    public static void main(String[] args) throws Exception {
        int resultCode = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(resultCode);
    }
}
