package com.cug.rpp4raster3d.util;

import com.cug.rpp4raster3d.spatialInputFormat.SpatialRecordReaderGroupRaster3D;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StopWatch;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class IOTest3 {

  public static void main(String[] argv) throws IOException, URISyntaxException, InterruptedException {

    String testDataDir = "hdfs://kvmmaster:9000/user/sparkl/rppo/raster3d-group232.dat";
    String testDataName = "r3d_raster3d-group232.dat_0_0_0";

    String filePath = testDataDir + "/" + testDataName;
    Configuration conf = new Configuration();
    conf.addResource(new Path("/home/sparkl/hadoop/etc/hadoop/hdfs-site.xml"));
    conf.addResource(new Path("/home/sparkl/hadoop/etc/hadoop/core-site.xml"));
    conf.addResource(new Path("/home/sparkl/hadoop/etc/hadoop/yarn-site.xml"));

    final Log LOG = LogFactory.getLog(IOTest3.class);
    LogManager.getLogger(IOTest3.class).setLevel(Level.DEBUG);
    LogManager.getLogger(SpatialRecordReaderGroupRaster3D.class).setLevel(Level.DEBUG);

//    FileSystem fs = FileSystem.get(conf);
    FileSystem fs = FileSystem.newInstance(new URI(filePath), conf);
    FSDataInputStream is = fs.open(new Path(filePath));

    Thread.sleep(5000); // sleep for remote debug
    System.out.println("reading start");
    StopWatch sw = new StopWatch();
    sw.start();
    byte[] buffer = new byte[1024];
    int total = 0;
    int tmp = 0;
    try {
      int readInt = 0;
      while (readInt != -1) {
        readInt = is.read(buffer);
        total += readInt;
        tmp += buffer[0];
      }
    } catch (EOFException e){
      LOG.debug("reading eof");
    }
    System.out.println("reading over, time: " + sw.now() / 1000 / 1000 / 1000.0);
    System.out.println("--- tmp value: " + tmp + "--- total read: " + total);
  }

}
