package com.acheao.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

public class TestHDFS {

    public Configuration configuration = null;

    public FileSystem fileSystem = null;

    @Before
    public void conn() throws IOException, InterruptedException {
        configuration = new Configuration(true);
        fileSystem = FileSystem.get(URI.create("hdfs://mycluster"), configuration, "hadoop");

    }

    @Test
    public void mkdir() throws IOException {
        Path path = new Path("/hadoop/test");
        if(fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        fileSystem.mkdirs(path);

    }

    @Test
    public void upload() throws IOException {
        BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(new File("./data/hello.txt")));
        Path path = new Path("/hadoop/test/hello.txt");
        FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
        IOUtils.copyBytes(inputStream, fsDataOutputStream, configuration, true);

    }


    @Test
    public void blocks() throws IOException {
        Path file = new Path("/hadoop/hello.txt");
        FileStatus fileStatus = fileSystem.getFileStatus(file);
        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation f : blockLocations){
            System.out.println(f);
        }

    }


    @After
    public void close() throws IOException {
        fileSystem.close();

    }


}
