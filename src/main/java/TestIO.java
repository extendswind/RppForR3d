import com.cug.rpp4raster3d.raster3d.Raster3D;
import com.cug.rpp4raster3d.spatialInputFormat.FileSplitGroupRaster3D;
import com.cug.rpp4raster3d.spatialInputFormat.InputSplitWritableRaster3D;
import com.cug.rpp4raster3d.spatialInputFormat.SpatialRecordReaderSimpleRaster3D;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.StopWatch;
import org.junit.Assert;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

public class TestIO {

  public static void recordReaderTimeTest() throws IOException {
    int cellXDim = 250;
    int cellYDim = 250;
    int cellZDim = 200;
    int cellXNum = 7;
    int cellYNum = 6;

    Configuration conf = new Configuration();
    //    conf.setInt("rpp4raster3d.spatial.radius", 5);
    SpatialRecordReaderSimpleRaster3D recordReader = new SpatialRecordReaderSimpleRaster3D();


    int fileXNum = 3;
    int fileYNum = 3;
    int fileZNum = 3;
    int radius = 10;

    // for first group
    Path[] paths = new Path[fileXNum * fileYNum * fileZNum];
    for (int z = 0; z < fileZNum; z++) {
      for (int y = 0; y < fileYNum; y++) {
        for (int x = 0; x < fileXNum; x++) {
          paths[x + y * fileXNum + z * fileXNum * fileYNum] =
              new Path("hdfs://kvmmaster:9000/user/sparkl/raster3d-2.dat/r3d_raster3d-2.dat" +
                  "_" + (x) + "_" + (y) + "_" + (z));
        }
      }
    }



    StopWatch sw = new StopWatch();
    sw.start();
    FileSplitGroupRaster3D inputSplit = new FileSplitGroupRaster3D(paths, 1, null, 0, cellXDim,
        cellYDim, cellZDim, fileXNum, fileYNum, fileZNum, radius);
    System.out.println(sw.now(TimeUnit.MICROSECONDS));
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    System.out.println(sw.now(TimeUnit.MICROSECONDS));
    recordReader.initialize(inputSplit, context);
    System.out.println(sw.now(TimeUnit.MICROSECONDS));
    recordReader.nextKeyValue();
    System.out.println(sw.now(TimeUnit.MICROSECONDS));
    sw.stop();

    // verify the value of only key of the split
    Raster3D value = recordReader.getCurrentValue();
    Assert.assertEquals(value.getXDim(), cellXDim + radius * 2);
    Assert.assertEquals(value.getYDim(), cellYDim + radius * 2);
    Assert.assertEquals(value.getZDim(), cellZDim + radius * 2);

    recordReader.close();
  }

  private static void simpleReadingTest () throws IOException, URISyntaxException{
        String pathStr = "hdfs://kvmmaster:9000/user/sparkl/raster3d-2.dat/r3d_raster3d-2.dat" +
        "_" + 0 + "_" + 0 + "_" + 0;
    Path tmpPath = new Path(pathStr);
    int cellXDim = 250;
    int cellYDim = 250;
    int cellZDim = 200;

    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY, "40960000");

    byte[] test = new byte[cellXDim*cellYDim*cellZDim];

    StopWatch sw = new StopWatch().start();
    DFSClient dfsClient = new DFSClient(new URI("hdfs://kvmmaster:9000"), conf);
    FSDataInputStream is = new FSDataInputStream(new BufferedFSInputStream(dfsClient.open("/user/sparkl/raster3d-2" +
        ".dat/r3d_raster3d-2.dat_0_0_0"), 4000));

    System.out.println(sw.now(TimeUnit.MICROSECONDS));
    sw.reset().start();


    is.readFully(0, test);
    //    for(int i=0; i<cellXDim*cellYDim*cellZDim * 10; i++){
    //      is.readByte();
    //    }

    System.out.println(sw.now(TimeUnit.MICROSECONDS));
    sw.stop();
  }

  public static void main(String[] argvs) throws IOException, URISyntaxException {
    recordReaderTimeTest();



    // 需要设置到 -Xmx3000m, 2500m都不够
//    int arrayNum = 1000000000;
//    byte[] array1 = new byte[arrayNum];
//    byte[] array2 = new byte[arrayNum];
//
//    for(int i=0; i<arrayNum; i++){
//      array1[i] = 1;
//      array2[i] = 2;
//    }
//
//    for(int i=0; i<arrayNum; i++){
//      array1[i] += array2[i];
//    }

  }
}
