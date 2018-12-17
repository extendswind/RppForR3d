package com.cug.geo3d.spatialInputFormat;

import com.cug.geo3d.upload.SpatialMapUploader;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.*;

import static org.junit.Assert.*;

public class TestSpatialRecordReader {

  // the whole grid is divided to 3*3 cells
  // each cell contains 30*30 pixel
  private int gridRowSize = 5;
  private int gridColSize = 5;
  private int cellRowSize = 200;
  private int cellColSize = 2000;
  private int radius = 5;


  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

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
    Assert.assertEquals(value.getWidth().get(), cellColSize + cellColSize / 2 + radius);
    Assert.assertEquals(value.getData()[0].get(), 0);
    Assert.assertEquals(value.getData()[1].get(), 1);
    Assert.assertEquals(value.getData()[1 + 2 * value.getWidth().get()].get(), 1 + 2 * gridColSize * cellColSize);

    Assert.assertEquals(recordReader.nextKeyValue(), false);
  }

  // for splitId = 1*gridColSize + 1
  @Test
  public void recordReadTest2() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt("geo3d.spatial.radius", 5);
    SpatialRecordReader recordReader = new SpatialRecordReader();

    Path[] paths = new Path[4];
    paths[0] = new Path("test_recordReader/test.dat_upload/grid_test.dat_1_1");
    paths[1] = new Path("test_recordReader/test.dat_upload/grid_test.dat_1_2");
    paths[2] = new Path("test_recordReader/test.dat_upload/grid_test.dat_2_1");
    paths[3] = new Path("test_recordReader/test.dat_upload/grid_test.dat_2_2");

    EdgeFileSplit inputSplit = new EdgeFileSplit(paths, 1, null, 1,
            gridRowSize, gridColSize, cellRowSize, cellColSize);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    recordReader.initialize(inputSplit, context);
    recordReader.nextKeyValue();

    LongWritable key = recordReader.getCurrentKey();
    Assert.assertEquals(key.get(), 1);

    // verify the value of only key of the split
    InputSplitWritable value = recordReader.getCurrentValue();
    Assert.assertEquals(value.getWidth().get(), cellColSize + radius * 2);

    // value.data[0] is the (cellRowSize + cellRowSize/2 - radius, cellColSize + cellColSize/2 - radius)
    Assert.assertEquals(value.getData()[0].get(), (cellRowSize + cellRowSize / 2 - radius) *
            cellColSize * gridColSize + cellColSize + cellColSize/2 - radius);
//    Assert.assertEquals(value.getData()[1].get(), 1);
//    Assert.assertEquals(value.getData()[1 + 2 * value.getWidth().get()].get(), 1 + 2 * gridColSize * cellColSize);

    Assert.assertEquals(recordReader.nextKeyValue(), false);
  }

  // for splitId == 1*gridRowSize + 0
  @Test
  public void recordReadTest3() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt("geo3d.spatial.radius", 5);
    SpatialRecordReader recordReader = new SpatialRecordReader();

    Path[] paths = new Path[4];
    paths[0] = new Path("test_recordReader/test.dat_upload/grid_test.dat_1_0");
    paths[1] = new Path("test_recordReader/test.dat_upload/grid_test.dat_1_1");
    paths[2] = new Path("test_recordReader/test.dat_upload/grid_test.dat_2_0");
    paths[3] = new Path("test_recordReader/test.dat_upload/grid_test.dat_2_1");

    EdgeFileSplit inputSplit = new EdgeFileSplit(paths, 1, null, 1,
            gridRowSize, gridColSize, cellRowSize, cellColSize);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    recordReader.initialize(inputSplit, context);
    recordReader.nextKeyValue();

    LongWritable key = recordReader.getCurrentKey();
    Assert.assertEquals(key.get(), 1);

    // verify the value of only key of the split
    InputSplitWritable value = recordReader.getCurrentValue();
    Assert.assertEquals(value.getWidth().get(), cellColSize + cellColSize/2 + radius);

    // value.data[0] is the (cellRowSize + cellRowSize/2 - radius, cellColSize + cellColSize/2 - radius)
    Assert.assertEquals(value.getData()[0].get(), (cellRowSize + cellRowSize / 2 - radius) *
            cellColSize * gridColSize );
//    Assert.assertEquals(value.getData()[1].get(), 1);
//    Assert.assertEquals(value.getData()[1 + 2 * value.getWidth().get()].get(), 1 + 2 * gridColSize * cellColSize);

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
  public void TestEdgeFileSplitWritable() throws IOException {
    Path[] files = new Path[2];
    files[0] = new Path("test1");
    files[1] = new Path("test2");
    String[] hosts = new String[2];
    hosts[0] = "host1";
    hosts[1] = "host2";

    EdgeFileSplit fileSplit = new EdgeFileSplit(files, 1, hosts, 2,
            3, 4, 5, 6);
    File tmpFile = tempFolder.newFile();
    FileSystem fs = FileSystem.getLocal(new Configuration());
    FSDataOutputStream out = fs.create(new Path(tmpFile.toString()), true);
    fileSplit.write(out);
    out.close();

    FSDataInputStream in = fs.open(new Path(tmpFile.toString()));
    EdgeFileSplit fileSplitRead = new EdgeFileSplit();
    fileSplitRead.readFields(in);
    in.close();

    Assert.assertEquals(fileSplitRead.getLength(), 1);
    Assert.assertEquals(true, fileSplitRead.getPaths()[0].toString().equals("test1"));
    Assert.assertEquals(fileSplitRead.getLocations()[0], "host1");
    Assert.assertEquals(fileSplitRead.getGridIndexInfo().cellColSize, 6);
    Assert.assertEquals(fileSplitRead.getSplitId(), 2);

  }


  @Test
  public void initialize() throws Exception {
  }

}