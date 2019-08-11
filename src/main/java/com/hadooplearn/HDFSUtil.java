package com.hadooplearn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUtil {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.124.166:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        boolean success = fileSystem.mkdirs(new Path("/root"));
        System.out.println(success);
    }
}
