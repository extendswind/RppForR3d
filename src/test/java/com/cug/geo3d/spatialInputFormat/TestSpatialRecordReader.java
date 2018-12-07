package com.cug.geo3d.spatialInputFormat;

import com.cug.geo3d.upload.SpatialMapUploader;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;

public class TestSpatialRecordReader {

  // the whole grid is divided to 3*3 cells
  // each cell contains 30*30 pixel
  private int gridRowSize = 3;
  private int gridColSize = 3;
  private int cellRowSize = 30;
  private int cellColSize = 30;
  private int radius = 5;


  @Test
  public void readSkipTest() throws IOException {
    DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream("test/test.dat")));
    System.out.println(inputStream.readInt());
    inputStream.skipBytes(400);
    System.out.println(inputStream.readInt());
  }

  @Before
  public void generateData() throws IOException {
    FileUtils.forceMkdir(new File("test_recordReader"));
    SpatialMapUploader.generateBinaryTestData("test_recordReader/test.dat",
            gridRowSize * cellRowSize, gridColSize * cellColSize);
    SpatialMapUploader.splitSpatialDataBinary("test_recordReader/test.dat",
            gridRowSize * cellRowSize, gridColSize * cellColSize,
            gridRowSize, gridColSize);
  }

  @After
  public void deleteTestData() throws IOException {
    FileUtil.fullyDelete(new File("test_recordReader"));
  }

  @Test
  public void recordReadTest() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt("geo3d.spatial.radius", 5);
    SpatialRecordReader recordReader = new SpatialRecordReader();

    Path[] paths = new Path[4];
    paths[0] = new Path("test_recordReader/test.dat_upload/grid_test.dat_0_0");
    paths[1] = new Path("test_recordReader/test.dat_upload/grid_test.dat_0_1");
    paths[2] = new Path("test_recordReader/test.dat_upload/grid_test.dat_1_0");
    paths[3] = new Path("test_recordReader/test.dat_upload/grid_test.dat_1_1");

    EdgeFileSplit inputSplit = new EdgeFileSplit(paths, 1, null, 0,
            gridRowSize, gridColSize, cellRowSize, cellColSize);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    recordReader.initialize(inputSplit, context);
    recordReader.nextKeyValue();

    LongWritable key = recordReader.getCurrentKey();
    Assert.assertEquals(key.get(), 0);

    // verify the value of only key of the split
    InputSplitWritable value = recordReader.getCurrentValue();
    Assert.assertEquals(value.getWidth().get(), cellColSize + cellColSize/2 + radius);
    Assert.assertEquals(value.getData()[0].get(), 0);
    Assert.assertEquals(value.getData()[1].get(), 1);
    Assert.assertEquals(value.getData()[1 + 2 * value.getWidth().get()].get(), 1 + 2*gridColSize*cellColSize);

    Assert.assertEquals(recordReader.nextKeyValue(), false);
  }


//   private Path createInputFile(Configuration conf, String data)
//      throws IOException {
//    FileSystem localFs = FileSystem.getLocal(conf);
//    Path file = new Path("test.txt");
//    Writer writer = new OutputStreamWriter(localFs.create(file));
//    try {
//      writer.write(data);
//    } finally {
//      writer.close();
//    }
//    return file;
//  }


  @Test
  public void initialize() throws Exception {
  }

}