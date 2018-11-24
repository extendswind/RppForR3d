package com.cug.geo3d.upload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;


public class hdfs_oper {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("dfs.blocksize", "1048576");
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://hadoopmaster:9000/"), conf);
        String test = conf.get("dfs.blocksize");

        BlockLocation[] locs = hdfs.getFileBlockLocations(new Path("/user/sparkl/test.dat"), 0, 10);

        for (BlockLocation loc: locs) {
          System.out.println(loc.toString());

        }

        Path listf = new Path("hdfs://hadoopmaster:9000/user/sparkl");
        FileStatus stats[] = hdfs.listStatus(listf);
        for (int i = 0; i < stats.length; ++i) {
            System.out.println(stats[i].getPath().toString());
        }

        //hdfs.delete(new Path("hdfs://tdh1:9000/testData"), true);
        hdfs.copyFromLocalFile(false, true,
                new Path("test.dat"),
                new Path("hdfs://hadoopmaster:9000/user/sparkl/"));

//        hdfs.create(new Path("hdfs://master:9000/test"));

        hdfs.close();
    }
}