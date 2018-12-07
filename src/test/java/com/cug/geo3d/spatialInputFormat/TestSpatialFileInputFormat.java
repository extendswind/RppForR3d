/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cug.geo3d.spatialInputFormat;

import com.cug.geo3d.util.GridCellInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(value = Parameterized.class)
public class TestSpatialFileInputFormat {

  private static final Log LOG = LogFactory.getLog(TestSpatialFileInputFormat.class);

  private static String testTmpDir = System.getProperty("test.build.data", "/tmp");
  private static final Path TEST_ROOT_DIR = new Path(testTmpDir, "TestFIF");

  private static FileSystem localFs;

  private int numThreads;

  // 下面 parameters标签下的 1、5 会作为参数构造两个 TestSpatialFileInputFormat
  // 后面的每一个test都会执行两次
  public TestSpatialFileInputFormat(int numThreads) {
    this.numThreads = numThreads;
    LOG.info("Running with numThreads: " + numThreads);
  }

  @Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][]{{1}, {5}};
    return Arrays.asList(data);
  }

  @Before
  public void setup() throws IOException {
    LOG.info("Using Test Dir: " + TEST_ROOT_DIR);
    localFs = FileSystem.getLocal(new Configuration());
    localFs.delete(TEST_ROOT_DIR, true);
    localFs.mkdirs(TEST_ROOT_DIR);
  }

  @After
  public void cleanup() throws IOException {
    localFs.delete(TEST_ROOT_DIR, true);
  }


  /////////////////////////////////

  private Configuration getSpatialConfiguration() {
    Configuration conf = new Configuration();
    conf.set("fs.test.impl.disable.cache", "true");
    conf.setClass("fs.test.impl", MockFileSystem.class, FileSystem.class);

    // grid size contained in filename
    conf.set(FileInputFormat.INPUT_DIR, "test:///spatial/test.dat/info_3_3_30_30");
    return conf;
  }

  @Test
  public void testSpatialFileInputFormat() throws IOException, InterruptedException {
    Configuration conf = getSpatialConfiguration();
    conf.set(FileInputFormat.INPUT_DIR_RECURSIVE, "true");
    Job job = Job.getInstance(conf);
    FileInputFormat<?, ?> fileInputFormat = new SpatialTextInputFormat();
    List<InputSplit> splits = fileInputFormat.getSplits(job);

    Assert.assertTrue(splits.get(0) instanceof EdgeFileSplit);
    Assert.assertEquals(splits.get(0).getLocations()[0], "d1" );
    Assert.assertEquals(splits.get(1).getLocations()[0], "d2" );


  }

  ////////////////////////////////////

//
//  @Test
//  public void testNumInputFilesRecursively() throws Exception {
//    Configuration conf = getConfiguration();
//    conf.set(FileInputFormat.INPUT_DIR_RECURSIVE, "true");
//    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);
//    Job job = Job.getInstance(conf);
//    FileInputFormat<?, ?> fileInputFormat = new TextInputFormat();
//    List<InputSplit> splits = fileInputFormat.getSplits(job);
//    Assert.assertEquals("Input splits are not correct", 3, splits.size());
//    verifySplits(Lists.newArrayList("test:/a1/a2/file2", "test:/a1/a2/file3",
//        "test:/a1/file1"), splits);
//
//    // Using the deprecated configuration
//    conf = getConfiguration();
//    conf.set("mapred.input.dir.recursive", "true");
//    job = Job.getInstance(conf);
//    splits = fileInputFormat.getSplits(job);
//    verifySplits(Lists.newArrayList("test:/a1/a2/file2", "test:/a1/a2/file3",
//        "test:/a1/file1"), splits);
//  }

//  @Test
//  public void testNumInputFilesWithoutRecursively() throws Exception {
//    Configuration conf = getConfiguration();
//    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);
//    Job job = Job.getInstance(conf);
//    FileInputFormat<?, ?> fileInputFormat = new TextInputFormat();
//    List<InputSplit> splits = fileInputFormat.getSplits(job);
//    Assert.assertEquals("Input splits are not correct", 2, splits.size());
//    verifySplits(Lists.newArrayList("test:/a1/a2", "test:/a1/file1"), splits);
//  }
//
//  @Test
//  public void testListLocatedStatus() throws Exception {
//    Configuration conf = getConfiguration();
//    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);
//    conf.setBoolean("fs.test.impl.disable.cache", false);
//    conf.set(FileInputFormat.INPUT_DIR, "test:///a1/a2");
//    MockFileSystem mockFs =
//        (MockFileSystem) new Path("test:///").getFileSystem(conf);
//    Assert.assertEquals("listLocatedStatus already called",
//        0, mockFs.numListLocatedStatusCalls);
//    Job job = Job.getInstance(conf);
//    FileInputFormat<?, ?> fileInputFormat = new TextInputFormat();
//    List<InputSplit> splits = fileInputFormat.getSplits(job);
//    Assert.assertEquals("Input splits are not correct", 2, splits.size());
//    Assert.assertEquals("listLocatedStatuss calls",
//        1, mockFs.numListLocatedStatusCalls);
//    FileSystem.closeAll();
//  }
//
//  @Test
//  public void testSplitLocationInfo() throws Exception {
//    Configuration conf = getConfiguration();
//    conf.set(FileInputFormat.INPUT_DIR,
//        "test:///a1/a2");
//    Job job = Job.getInstance(conf);
//    TextInputFormat fileInputFormat = new TextInputFormat();
//    List<InputSplit> splits = fileInputFormat.getSplits(job);
//    String[] locations = splits.get(0).getLocations();
//    Assert.assertEquals(2, locations.length);
//    SplitLocationInfo[] locationInfo = splits.get(0).getLocationInfo();
//    Assert.assertEquals(2, locationInfo.length);
//    SplitLocationInfo localhostInfo = locations[0].equals("localhost") ?
//        locationInfo[0] : locationInfo[1];
//    SplitLocationInfo otherhostInfo = locations[0].equals("otherhost") ?
//        locationInfo[0] : locationInfo[1];
//    Assert.assertTrue(localhostInfo.isOnDisk());
//    Assert.assertTrue(localhostInfo.isInMemory());
//    Assert.assertTrue(otherhostInfo.isOnDisk());
//    Assert.assertFalse(otherhostInfo.isInMemory());
//  }

//  @Test
//  public void testListStatusSimple() throws IOException {
//    Configuration conf = new Configuration();
//    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);
//
//    List<Path> expectedPaths = configureTestSimple(conf, localFs);
//
//    Job job  = Job.getInstance(conf);
//    FileInputFormat<?, ?> fif = new TextInputFormat();
//    List<FileStatus> statuses = fif.listStatus(job);
//
//    verifyFileStatuses(expectedPaths, statuses, localFs);
//  }

//  @Test
//  public void testListStatusNestedRecursive() throws IOException {
//    Configuration conf = new Configuration();
//    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);
//
//    List<Path> expectedPaths = configureTestNestedRecursive(conf, localFs);
//    Job job  = Job.getInstance(conf);
//    FileInputFormat<?, ?> fif = new TextInputFormat();
//    List<FileStatus> statuses = fif.listStatus(job);
//
//    verifyFileStatuses(expectedPaths, statuses, localFs);
//  }


//  @Test
//  public void testListStatusNestedNonRecursive() throws IOException {
//    Configuration conf = new Configuration();
//    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);
//
//    List<Path> expectedPaths = configureTestNestedNonRecursive(conf, localFs);
//    Job job  = Job.getInstance(conf);
//    FileInputFormat<?, ?> fif = new TextInputFormat();
//    List<FileStatus> statuses = fif.listStatus(job);
//
//    verifyFileStatuses(expectedPaths, statuses, localFs);
//  }

//  @Test
//  public void testListStatusErrorOnNonExistantDir() throws IOException {
//    Configuration conf = new Configuration();
//    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);
//
//    configureTestErrorOnNonExistantDir(conf, localFs);
//    Job job  = Job.getInstance(conf);
//    FileInputFormat<?, ?> fif = new TextInputFormat();
//    try {
//      fif.listStatus(job);
//      Assert.fail("Expecting an IOException for a missing Input path");
//    } catch (IOException e) {
//      Path expectedExceptionPath = new Path(TEST_ROOT_DIR, "input2");
//      expectedExceptionPath = localFs.makeQualified(expectedExceptionPath);
//      Assert.assertTrue(e instanceof InvalidInputException);
//      Assert.assertEquals(
//          "Input path does not exist: " + expectedExceptionPath.toString(),
//          e.getMessage());
//    }
//  }

  //  public static List<Path> configureTestSimple(Configuration conf, FileSystem localFs)
//      throws IOException {
//    Path base1 = new Path(TEST_ROOT_DIR, "input1");
//    Path base2 = new Path(TEST_ROOT_DIR, "input2");
//    conf.set(FileInputFormat.INPUT_DIR,
//        localFs.makeQualified(base1) + "," + localFs.makeQualified(base2));
//    localFs.mkdirs(base1);
//    localFs.mkdirs(base2);
//
//    Path in1File1 = new Path(base1, "file1");
//    Path in1File2 = new Path(base1, "file2");
//    localFs.createNewFile(in1File1);
//    localFs.createNewFile(in1File2);
//
//    Path in2File1 = new Path(base2, "file1");
//    Path in2File2 = new Path(base2, "file2");
//    localFs.createNewFile(in2File1);
//    localFs.createNewFile(in2File2);
//    List<Path> expectedPaths = Lists.newArrayList(in1File1, in1File2, in2File1,
//        in2File2);
//    return expectedPaths;
//  }
//
//  public static List<Path> configureTestNestedRecursive(Configuration conf,
//      FileSystem localFs) throws IOException {
//    Path base1 = new Path(TEST_ROOT_DIR, "input1");
//    conf.set(FileInputFormat.INPUT_DIR,
//        localFs.makeQualified(base1).toString());
//    conf.setBoolean(
//        FileInputFormat.INPUT_DIR_RECURSIVE,
//        true);
//    localFs.mkdirs(base1);
//
//    Path inDir1 = new Path(base1, "dir1");
//    Path inDir2 = new Path(base1, "dir2");
//    Path inFile1 = new Path(base1, "file1");
//
//    Path dir1File1 = new Path(inDir1, "file1");
//    Path dir1File2 = new Path(inDir1, "file2");
//
//    Path dir2File1 = new Path(inDir2, "file1");
//    Path dir2File2 = new Path(inDir2, "file2");
//
//    localFs.mkdirs(inDir1);
//    localFs.mkdirs(inDir2);
//
//    localFs.createNewFile(inFile1);
//    localFs.createNewFile(dir1File1);
//    localFs.createNewFile(dir1File2);
//    localFs.createNewFile(dir2File1);
//    localFs.createNewFile(dir2File2);
//
//    List<Path> expectedPaths = Lists.newArrayList(inFile1, dir1File1,
//        dir1File2, dir2File1, dir2File2);
//    return expectedPaths;
//  }
//
//  public static List<Path> configureTestNestedNonRecursive(Configuration conf,
//      FileSystem localFs) throws IOException {
//    Path base1 = new Path(TEST_ROOT_DIR, "input1");
//    conf.set(FileInputFormat.INPUT_DIR,
//        localFs.makeQualified(base1).toString());
//    conf.setBoolean(
//        FileInputFormat.INPUT_DIR_RECURSIVE,
//        false);
//    localFs.mkdirs(base1);
//
//    Path inDir1 = new Path(base1, "dir1");
//    Path inDir2 = new Path(base1, "dir2");
//    Path inFile1 = new Path(base1, "file1");
//
//    Path dir1File1 = new Path(inDir1, "file1");
//    Path dir1File2 = new Path(inDir1, "file2");
//
//    Path dir2File1 = new Path(inDir2, "file1");
//    Path dir2File2 = new Path(inDir2, "file2");
//
//    localFs.mkdirs(inDir1);
//    localFs.mkdirs(inDir2);
//
//    localFs.createNewFile(inFile1);
//    localFs.createNewFile(dir1File1);
//    localFs.createNewFile(dir1File2);
//    localFs.createNewFile(dir2File1);
//    localFs.createNewFile(dir2File2);
//
//    List<Path> expectedPaths = Lists.newArrayList(inFile1, inDir1, inDir2);
//    return expectedPaths;
//  }
//
//  public static List<Path> configureTestErrorOnNonExistantDir(Configuration conf,
//      FileSystem localFs) throws IOException {
//    Path base1 = new Path(TEST_ROOT_DIR, "input1");
//    Path base2 = new Path(TEST_ROOT_DIR, "input2");
//    conf.set(FileInputFormat.INPUT_DIR,
//        localFs.makeQualified(base1) + "," + localFs.makeQualified(base2));
//    conf.setBoolean(
//        FileInputFormat.INPUT_DIR_RECURSIVE,
//        true);
//    localFs.mkdirs(base1);
//
//    Path inFile1 = new Path(base1, "file1");
//    Path inFile2 = new Path(base1, "file2");
//
//    localFs.createNewFile(inFile1);
//    localFs.createNewFile(inFile2);
//
//    List<Path> expectedPaths = Lists.newArrayList();
//    return expectedPaths;
//  }
//
//  public static void verifyFileStatuses(List<Path> expectedPaths,
//      List<FileStatus> fetchedStatuses, final FileSystem localFs) {
//    Assert.assertEquals(expectedPaths.size(), fetchedStatuses.size());
//
//    Iterable<Path> fqExpectedPaths = Iterables.transform(expectedPaths,
//        new Function<Path, Path>() {
//          @Override
//          public Path apply(Path input) {
//            return localFs.makeQualified(input);
//          }
//        });
//
//    Set<Path> expectedPathSet = Sets.newHashSet(fqExpectedPaths);
//    for (FileStatus fileStatus : fetchedStatuses) {
//      if (!expectedPathSet.remove(localFs.makeQualified(fileStatus.getPath()))) {
//        Assert.fail("Found extra fetched status: " + fileStatus.getPath());
//      }
//    }
//    Assert.assertEquals(
//        "Not all expectedPaths matched: " + expectedPathSet.toString(), 0,
//        expectedPathSet.size());
//  }
//
//
//  private void verifySplits(List<String> expected, List<InputSplit> splits) {
//    Iterable<String> pathsFromSplits = Iterables.transform(splits,
//        new Function<InputSplit, String>() {
//          @Override
//          public String apply(@Nullable InputSplit input) {
//            return ((FileSplit) input).getPath().toString();
//          }
//        });
//
//    Set<String> expectedSet = Sets.newHashSet(expected);
//    for (String splitPathString : pathsFromSplits) {
//      if (!expectedSet.remove(splitPathString)) {
//        Assert.fail("Found extra split: " + splitPathString);
//      }
//    }
//    Assert.assertEquals(
//        "Not all expectedPaths matched: " + expectedSet.toString(), 0,
//        expectedSet.size());
//  }
//
  private Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.set("fs.test.impl.disable.cache", "true");
    conf.setClass("fs.test.impl", MockFileSystem.class, FileSystem.class);
    conf.set(FileInputFormat.INPUT_DIR, "test:///a1");
    return conf;
  }

  static class MockFileSystem extends RawLocalFileSystem {
    int numListLocatedStatusCalls = 0;

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException,
            IOException {
      if (f.toString().equals("test:/a1")) {
        return new FileStatus[]{
                new FileStatus(0, true, 1, 150,
                        150, new Path("test:/a1/a2")),
                new FileStatus(10, false, 1, 150,
                        150, new Path("test:/a1/file1"))};
      } else if (f.toString().equals("test:/a1/a2")) {
        return new FileStatus[]{
                new FileStatus(10, false, 1, 150, 150,
                        new Path("test:/a1/a2/file2")),
                new FileStatus(10, false, 1, 151, 150,
                        new Path("test:/a1/a2/file3"))};
      } else if (f.toString().equals("test:/spatial/test.dat")) {
        return new FileStatus[]{
                new FileStatus(10, false, 1, 150, 150,
                        new Path("test:/spatial/grid_test.dat/grid_test.dat_0_0")),
                new FileStatus(10, false, 1, 150, 150,
                        new Path("test:/spatial/grid_test.dat/grid_test.dat_0_1")),
                new FileStatus(10, false, 1, 150, 150,
                        new Path("test:/spatial/grid_test.dat/grid_test.dat_1_0")),
                new FileStatus(10, false, 1, 150, 150,
                        new Path("test:/spatial/grid_test.dat/grid_test.dat_1_1")),};
      }
      return new FileStatus[0];
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
            throws IOException {
      return new FileStatus[]{new FileStatus(10, true, 1, 150, 150,
              pathPattern)};
    }

    @Override
    public FileStatus[] listStatus(Path f, PathFilter filter)
            throws FileNotFoundException, IOException {
      return this.listStatus(f);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path path, long start, long len)
            throws IOException {
      GridCellInfo cellInfo = new GridCellInfo();
      GridCellInfo.getGridIndexFromFilename(path.getName(), cellInfo);


//        BlockLocation blockLocationD1 = new BlockLocation(new String[]{"d0:50010", "d1:50010"},
//                        new String[]{"d0", "d1"}, new String[]{"d1"},
//                        new String[0], 0, len, false);
//      BlockLocation blockLocationD2 =   new BlockLocation(new String[]{"d1:50010", "d2:50010"},
//                        new String[]{"d1", "d2"}, new String[]{"d1"},
//                        new String[0], 0, len, false);
//      BlockLocation blockLocationD3 =                 new BlockLocation(new String[]{"d2:50010", "d3:50010"},
//                        new String[]{"d2", "d3"}, new String[]{"d2"},
//                        new String[0], 0, len, false);

      // 暂以列为单位划分到数据节点
      if (cellInfo.colId == 0) {
        return new BlockLocation[]{
                new BlockLocation(new String[]{"d0:50010", "d1:50010"},
                        new String[]{"d0", "d1"}, new String[]{"d1"},
                        new String[0], 0, len, false)};
      }if (cellInfo.colId == 1) {
        return new BlockLocation[]{
                new BlockLocation(new String[]{"d1:50010", "d2:50010"},
                        new String[]{"d1", "d2"}, new String[]{"d1"},
                        new String[0], 0, len, false)};
      } else {
        return new BlockLocation[]{
                new BlockLocation(new String[]{"d2:50010", "d3:50010"},
                        new String[]{"d2", "d3"}, new String[]{"d2"},
                        new String[0], 0, len, false)};
      }
    }

    // TODO not use??
    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
            throws IOException {
      GridCellInfo cellInfo = new GridCellInfo();
      GridCellInfo.getGridIndexFromFilename(file.getPath().getName(), cellInfo);

      // 暂以列为单位划分到数据节点
      if (cellInfo.colId == 0) {
        return new BlockLocation[]{
                new BlockLocation(new String[]{"d0:50010", "d1:50010"},
                        new String[]{"d0", "d1"}, new String[]{"d1"},
                        new String[0], 0, len, false)};
      }if (cellInfo.colId == 1) {
        return new BlockLocation[]{
                new BlockLocation(new String[]{"d1:50010", "d2:50010"},
                        new String[]{"d1", "d2"}, new String[]{"d1"},
                        new String[0], 0, len, false)};
      } else {
        return new BlockLocation[]{
                new BlockLocation(new String[]{"d2:50010", "d3:50010"},
                        new String[]{"d2", "d3"}, new String[]{"d2"},
                        new String[0], 0, len, false)};
      }
    }

    @Override
    protected RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f,
                                                                  PathFilter filter) throws FileNotFoundException, IOException {
      ++numListLocatedStatusCalls;
      return super.listLocatedStatus(f, filter);
    }
  }
}
