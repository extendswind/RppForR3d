package com.cug.rpp4raster3d.util;

import com.cug.rpp4raster3d.raster3d.NormalRaster3D;
import com.cug.rpp4raster3d.spatialInputFormat.FileSplitGroupRaster3D;
import com.cug.rpp4raster3d.spatialInputFormat.SpatialRecordReaderGroupRaster3D;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.*;
import org.apache.log4j.spi.Configurator;
import org.junit.Assert;

import java.io.IOException;

public class IOTest {

  private int cellZNum = 4;
  private int cellYNum = 9;
  private int cellXNum = 8;
  private int cellXDim = 250;
  private int cellYDim = 200;
  private int cellZDim = 200;
  private int globalXDim = cellXDim * cellXNum;
  private int globalYDim = cellYDim * cellYNum;

  private GroupInfo groupInfo = GroupInfo.getDefaultGroupInfo();

  private int radius = 7;

  private String testDataDir = "hdfs://kvmmaster:9000/user/sparkl/rppo/raster3d-group232.dat";
  private String testDataName = "raster3d-group232.dat";
  private Configuration conf;

  public static void main(String[] argv) throws IOException {

    final Log LOG = LogFactory.getLog(IOTest.class);
//    Logger root = LogManager.getRootLogger();
//    root.removeAllAppenders();
//    root.addAppender(new ConsoleAppender(new SimpleLayout(), "System.out"));
//    LogManager.getRootLogger().setLevel(Level.DEBUG);
//    LogManager.getLogger(IOTest.class).setLevel(Level.DEBUG);
//    System.out.println(root.getAllAppenders().toString());
    LogManager.getLogger(IOTest.class).setLevel(Level.DEBUG);
    LogManager.getLogger(SpatialRecordReaderGroupRaster3D.class).setLevel(Level.DEBUG);

    LOG.debug("test begin");
    IOTest ioTest = new IOTest();
    ioTest.conf = new Configuration();
    ioTest.conf.addResource(new Path("/home/sparkl/hadoop/etc/hadoop/hdfs-site.xml"));
    ioTest.conf.addResource(new Path("/home/sparkl/hadoop/etc/hadoop/core-site.xml"));
    ioTest.conf.addResource(new Path("/home/sparkl/hadoop/etc/hadoop/yarn-site.xml"));

    System.out.println("system out string");
    ioTest.firstGroupTest();
  }

  private void firstGroupTest() throws IOException {
    testRecordReaderForNormalGroup(2, 0, 0, 0, 0, 0, 0);
  }

  private void testRecordReaderForNormalGroup(int groupZSize, int fileXStart, int fileYStart, int fileZStart,
                                              int valueXBegin,
                                              int valueYBegin, int valueZBegin) throws IOException {
    conf.setInt("rpp4raster3d.spatial.radius", 7);
    SpatialRecordReaderGroupRaster3D recordReader = new SpatialRecordReaderGroupRaster3D();

    // for first group
    Path[] paths = new Path[groupInfo.rowSize * groupInfo.colSize * groupZSize];
    for (int z = 0; z < groupZSize; z++) {
      for (int y = 0; y < groupInfo.rowSize; y++) {
        for (int x = 0; x < groupInfo.colSize; x++) {
          paths[x + y * groupInfo.colSize + z * groupInfo.rowSize * groupInfo.colSize] =
              new Path(testDataDir + "/"  + SpatialConstant.RASTER_3D_INDEX_PREFIX + "_" +
                  testDataName + "_" + (x + fileXStart) + "_" + (y + fileYStart) + "_" + (z + fileZStart));
        }
      }
    }
    boolean isFirstZGroup = false;
    if (valueZBegin == 0) {
      isFirstZGroup = true;
    }

    FileSplitGroupRaster3D inputSplit = new FileSplitGroupRaster3D(paths, 1, null, 0, cellXDim, cellYDim, cellZDim,
        groupInfo.colSize, groupInfo.rowSize, groupZSize, isFirstZGroup, radius);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    recordReader.initialize(inputSplit, context);
    recordReader.nextKeyValue();
    LongWritable key = recordReader.getCurrentKey();
    Assert.assertEquals(key.get(), 0);
    NormalRaster3D raster3D = (NormalRaster3D) recordReader.getCurrentValue();
    System.out.println(raster3D.getXDim() + " " + raster3D.getYDim() + " " + raster3D.getZDim());
    recordReader.close();
  }


}
