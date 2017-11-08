package com.mutou.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Created By sk-tengfeiwang on 2017/11/2.
 */
public class TestHDFS {
    private static String dir = "/tmp/testhdfsdir";
    private static String targetFile = "/write_test_file";
    private static Configuration conf = new Configuration();

    static {
        conf.set("fs.default.name", "hdfs://192.168.11.2:9000");
    }

    public static void main(String[] args) throws Exception {
        writeFile();
        readFile();
    }

    public static void writeFile() throws Exception {
        FileSystem fs = FileSystem.get(conf);

        //创建目录
        Path tmpPath = new Path(dir);
        if (!fs.exists(tmpPath)) {
            fs.mkdirs(tmpPath);
        }

        FSDataOutputStream outputStream = fs.create(new Path(dir + targetFile), true);

        for (int i = 0; i < 10; i++) {
            outputStream.writeBytes("the first hdfs write file" + i + "\n");
        }
        outputStream.close();
    }

    public static void readFile() throws Exception {
        FileSystem fs = FileSystem.get(conf);

        Path path = new Path(dir + targetFile);
        FSDataInputStream inputStream = fs.open(path);

        BufferedReader bf = new BufferedReader(new InputStreamReader(inputStream));
        String info = null;
        while ((info = bf.readLine()) != null) {
            System.out.println(info);
        }

        inputStream.close();
    }
}
