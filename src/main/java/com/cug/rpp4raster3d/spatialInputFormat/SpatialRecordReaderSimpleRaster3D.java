package com.cug.rpp4raster3d.spatialInputFormat;

/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  <p>
  http://www.apache.org/licenses/LICENSE-2.0
  <p>
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */


import com.cug.rpp4raster2d.inputFormat.InputSplitWritable;
import com.cug.rpp4raster3d.raster3d.*;
import com.cug.rpp4raster3d.util.SpatialConstant;
import com.google.gson.annotations.Since;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StopWatch;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;


// input : EdgeInputSplit
// output : key(left up position of inputSplit)
//          value(width/height/value)

/**
 * 将InputSplit解析成键值对
 * 一个inputSplit只得到一个键值对，其中key为splitId，value为{@link InputSplitWritable}，记录所在splitId的宽，高和具体数据
 * 一种比较省内存的简单resampling实现，后期可以改为更通用的操作。
 * 输入一个文件以及周边的文件，按层每次读一部分以降低内存占用。
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Evolving
@Since(1.8)
public class SpatialRecordReaderSimpleRaster3D extends RecordReader<LongWritable, Raster3D> {


  /**
   * id and file of every cell in the grid
   * mainly used in BlockPlacementPolicyDefaultSpatial for *getting cell info from filename*
   * designed for 2D and add zId for 3D
   */
  static class CellIndexInfoForDefaultPlacement {
    public int rowId;
    public int colId;
    public int zId;
    public String filename; // 不带grid编号的文件名
    public String filepath;

    /**
     * 通过文件名判断是否为grid index 或者 raster3d
     * filename example: (grid) grid_filename_0_1   (raster 3d) raster3d_0_1_1
     *
     * @param srcFile 带路径的文件名
     * @return grid index时返回对象 否则null
     */
    public static CellIndexInfoForDefaultPlacement getGridCellInfoFromFilename(String srcFile) {
      String[] filenameSplit = FilenameUtils.getName(srcFile).split("_");
      CellIndexInfoForDefaultPlacement pos = new CellIndexInfoForDefaultPlacement();
      if (filenameSplit.length == 5 && filenameSplit[0].equals(SpatialConstant.Default_3D_PLACEMENT_PREFIX)) {
        pos.colId = Integer.parseInt(filenameSplit[2]);
        pos.rowId = Integer.parseInt(filenameSplit[3]);
        pos.zId = Integer.parseInt(filenameSplit[4]);
        pos.filename = filenameSplit[1];
        pos.filepath = FilenameUtils.getPath(srcFile);
        return pos;
      } else {
        return null;
      }
    }

  }


  private static final Log LOG = LogFactory.getLog(SpatialRecordReaderSimpleRaster3D.class);

  public int radius; // analysis radius

  private DataInputStream[] inputStreams;  // for reading data
  private HdfsDataInputStream[] dfsInputStreams; //  for reading statistics
  private Path[] paths;
  //  private FSDataInputStream[] fsDataInputStreams;  // for reset the read position
  boolean isLocalFileSystem;
  private LongWritable key;
  private Raster3D value;
  //  private InputSplitWritableRaster3D value;
  private FileSplitGroupRaster3D inputSplit;
  private Configuration conf;

  private int cellXDim;
  private int cellYDim;
  private int cellZDim;

  // number of files in x dimension in the group
  // the group means the center file and surround files.
  // usually, groupXSize is 2 for first and last group in a dimension
  private int groupXSize;
  private int groupYSize;
  private int groupZSize;
  CellIndexInfoForDefaultPlacement cellIndexInfo;

  private int splitId;
  private boolean isTest;

  private IOStatics ioStatics;

  public static final String RECORD_READER_IS_TEST_KEY = "r3d.simple.recordreader.test.boolean";

  public SpatialRecordReaderSimpleRaster3D() {

  }


  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    inputSplit = (FileSplitGroupRaster3D) genericSplit;
    conf = context.getConfiguration();
    paths = inputSplit.getPaths();

    // open the file and seek to the start of the inputSplit
    final FileSystem fs = paths[0].getFileSystem(conf);
    isLocalFileSystem = false;
    // io statistic is ignored for local file system
    if (fs instanceof LocalFileSystem) {
      isLocalFileSystem = true;
    } else {
      dfsInputStreams = new HdfsDataInputStream[paths.length];

      // 为了获取voxel size
      Raster3D raster3DTmp = Raster3dFactory.getRaster3D(conf, 1, 1, 1);
      ioStatics = new IOStatics(inputSplit.getLocations()[0], inputSplit.splitId, fs, raster3DTmp.getVoxelSize());
    }
    inputStreams = new DataInputStream[paths.length];
    for (int i = 0; i < paths.length; i++) {
      FSDataInputStream fsDataInputStream = fs.open(paths[i]);
      if (!isLocalFileSystem) {
        dfsInputStreams[i] = (HdfsDataInputStream) fsDataInputStream;
        // 使用默认副本放置策略时，buffer会使IO开销明显增大几倍
//        inputStreams[i] = new DataInputStream(new BufferedInputStream(dfsInputStreams[i]));
        inputStreams[i] = dfsInputStreams[i];
      } else {
        inputStreams[i] = new DataInputStream(new BufferedInputStream(fsDataInputStream));
      }
    }

    radius = inputSplit.radius;
    cellXDim = inputSplit.cellXDim;
    cellYDim = inputSplit.cellYDim;
    cellZDim = inputSplit.cellZDim;
    groupXSize = inputSplit.groupXSize;
    groupYSize = inputSplit.groupYSize;
    groupZSize = inputSplit.groupZSize;
    cellIndexInfo = CellIndexInfoForDefaultPlacement.getGridCellInfoFromFilename(paths[0].toString());

    splitId = inputSplit.splitId;
    isTest = conf.getBoolean(RECORD_READER_IS_TEST_KEY, false);
  }


  public boolean nextKeyValue() {
    if (key == null) {
      key = new LongWritable(inputSplit.splitId);
    } else {
      return false;
    }

    StopWatch sw = new StopWatch().start();

    // 如果为x方向的第一个或者最后一个，只需要从x方向的一个文件读一个半径内的数据；否则，要从周边两个方向读取。
    int valueXDim = (groupXSize - 1) * radius + cellXDim;
    int valueYDim = (groupYSize - 1) * radius + cellYDim;
    int valueZDim = (groupZSize - 1) * radius + cellZDim;

    Raster3D raster3D = Raster3dFactory.getRaster3D(conf, valueXDim, valueYDim, valueZDim);
    readFromStreams(raster3D);

    value = raster3D;
    sw.stop();
    LOG.debug("data reading time of RecordReader lsakdjfl is " + sw.now(TimeUnit.SECONDS));

    if (LOG instanceof Log4JLogger) {
      LOG.debug(((Log4JLogger) LOG).getLogger().getAllAppenders().toString());
    }
    return true;
  }


  // 从所有的stream中读想要的部分
  void readFromStreams(Raster3D raster3D) {
    // for every file -------------
    int groupXEnd = 1;
    int groupYEnd = 1;
    int groupZEnd = 1;

    // group的所有文件中，开始文件的位置
    // group中的文件以中心文件为(0,0,0)，后面几个循环确定相对坐标
    int[] groupStart = new int[]{-1, -1, -1};
    if (groupXSize == 2) {
      if (cellIndexInfo.colId == 0) {
        groupStart[0] = 0;
      } else {
        groupXEnd = 0;
      }
    }
    if (groupYSize == 2) {
      if (cellIndexInfo.rowId == 0) {
        groupStart[1] = 0;
      } else {
        groupYEnd = 0;
      }
    }
    if (groupZSize == 2) {
      if (cellIndexInfo.zId == 0) {
        groupStart[2] = 0;
      } else {
        groupZEnd = 0;
      }
    }

    int[] startPos = new int[3]; // startX startY startZ   reading start position in current file
    int[] lengths = new int[3]; // lengthX lengthY lengthZ  reading length
    int[] toValuePos = new int[3]; // toValueX toValueY toValueZ    position in value to save reading data

    int[] filePos = new int[3];
    int[] cellDims = new int[]{cellXDim, cellYDim, cellZDim};

    // 对于group中的每个文件
    for (filePos[2] = groupStart[2]; filePos[2] <= groupZEnd; filePos[2]++) {
      for (filePos[1] = groupStart[1]; filePos[1] <= groupYEnd; filePos[1]++) {
        for (filePos[0] = groupStart[0]; filePos[0] <= groupXEnd; filePos[0]++) {

          // calculate startPos, lengths, toValuePos,  数组分别对应x y z
          for (int i = 0; i < 3; i++) {
            if (filePos[i] == -1) {
              startPos[i] = cellDims[i] - radius;
              lengths[i] = radius;
              toValuePos[i] = 0;
            } else if (filePos[i] == 0) {
              startPos[i] = 0;
              lengths[i] = cellDims[i];
              if (groupStart[i] == -1) {
                toValuePos[i] = radius;
              } else {
                toValuePos[i] = 0;
              }
            } else if (filePos[i] == 1) {
              startPos[i] = 0;
              lengths[i] = radius;
              if (groupStart[i] == -1) {
                toValuePos[i] = radius + cellDims[i];
              } else {
                toValuePos[i] = cellDims[i];
              }
            }
          }

          int cellSize = raster3D.getVoxelSize();
          try {
            int streamIndex = (filePos[2] - groupStart[2]) * groupXSize * groupYSize +
                (filePos[1] - groupStart[1]) * groupXSize + (filePos[0] - groupStart[0]);
            if (!isLocalFileSystem) {
              ioStatics.addRemoteReadIO(paths[streamIndex], lengths[0], lengths[1], lengths[2]);
            }
            readPartFromStream(streamIndex, cellXDim, cellYDim, startPos[0], startPos[1], startPos[2],
                lengths[0], lengths[1], lengths[2], cellSize, raster3D, raster3D.getXDim(), raster3D.getYDim(),
                toValuePos[0], toValuePos[1], toValuePos[2]);
          } catch (IOException e) {
            e.printStackTrace();
            LOG.error("file reading error! key: " + splitId + ", error file number: " +
                ((filePos[2] - groupStart[2]) * groupXSize * groupYSize +
                    (filePos[1] - groupStart[1]) * groupXSize + (filePos[0] - groupStart[0])));
          } catch (ArrayIndexOutOfBoundsException e) {
            e.printStackTrace();
          }
        }
      }
    }
  }

  /**
   * 一个辅助函数
   * 从一个文件（InputStream）中读取一部分到目标数组
   * 为了降低前面6重for循环的复杂度
   */
  void readPartFromStream(int inputStreamId, int cellXDim, int cellYDim,
                          int startX, int startY, int startZ, int lengthX, int lengthY, int lengthZ,
                          int cellAttrSize,
                          Raster3D raster3D, int valueXDim, int valueYDim,
                          int toValueX, int toValueY, int toValueZ) throws IOException {
    StopWatch sw = new StopWatch();
    sw.start();
    int ioCount = 0;
    int ioCount2 = 0;
    enforceSkip(inputStreams[inputStreamId], (long) cellXDim * cellYDim * startZ * cellAttrSize);
    ioCount2 += (long) cellXDim * cellYDim * startZ * cellAttrSize;
    ioCount += cellXDim * cellYDim * startZ * cellAttrSize;
    for (int zz = 0; zz < lengthZ; zz++) {
      enforceSkip(inputStreams[inputStreamId], (long) cellXDim * startY * cellAttrSize);
      ioCount2 += ((long) cellXDim * startY * cellAttrSize);
      ioCount += cellXDim * startY * cellAttrSize;
      for (int yy = 0; yy < lengthY; yy++) {
        enforceSkip(inputStreams[inputStreamId], (long) startX * cellAttrSize);
        ioCount2 += ((long) startX * cellAttrSize);
        ioCount += startX * cellAttrSize;
        for (int xx = 0; xx < lengthX; xx++) {
          raster3D.readAttr(toValueX + xx +
                  (toValueY + yy) * valueXDim
                  + (toValueZ + zz) * valueXDim * valueYDim
              , inputStreams[inputStreamId]);
          ioCount += cellAttrSize;
          ioCount2 += cellAttrSize;
        }
        enforceSkip(inputStreams[inputStreamId], ((long) (cellXDim - lengthX - startX) * cellAttrSize));
        ioCount2 += ((long) (cellXDim - lengthX - startX) * cellAttrSize);
        ioCount += (cellXDim - lengthX - startX) * cellAttrSize;
      }
      ioCount2 += (long) (cellYDim - lengthY - startY) * cellXDim * cellAttrSize;
      enforceSkip(inputStreams[inputStreamId], (long) (cellYDim - lengthY - startY) * cellXDim * cellAttrSize);
      ioCount += (cellYDim - lengthY - startY) * cellXDim * cellAttrSize;
      if (ioCount != ioCount2) {
        System.out.println("test");
      }
    }
    // is null if open local file (for unit test)
    if (dfsInputStreams != null) {
      HdfsDataInputStream hdfsDataInputStream = dfsInputStreams[inputStreamId];
      long remoteReading = hdfsDataInputStream.getReadStatistics().getRemoteBytesRead();
      long totalReading = hdfsDataInputStream.getReadStatistics().getTotalBytesRead();
      long localReading = hdfsDataInputStream.getReadStatistics().getTotalShortCircuitBytesRead();
      String ioStatics =
          "fileId: " + inputStreamId + " -- remote: " + remoteReading / 1024 / 1024 + " -- total: " +
              totalReading / 1024 / 1024 + " " + "-- " + "short circuit: " + localReading / 1024 / 1024;
      LOG.debug(ioStatics);
    }
    LOG.debug("inputStream read time: " + sw.now() / 1000 / 1000 / 1000.0 + "s");
  }

  private void enforceSkip(InputStream inputStream, long n) {
    try {
      long skip;
      long remain = n;
      do {
        skip = inputStream.skip(remain);
        remain -= skip;
      } while (remain != 0);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public Raster3D getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the inputSplit
   */
  @Override
  public float getProgress() {
    if (key == null) {
      return 0.0f;
    } else {
      return 1.0f;
    }
  }

  @Override
  public synchronized void close() {
    try {
      LOG.debug("Record reader close and write IO statistics to HDFS");
      if (inputStreams != null) {
        for (int i = 0; i < inputStreams.length; i++) {
          inputStreams[i].close();
        }
      }

      // write to local file
      if (!isLocalFileSystem && dfsInputStreams != null) {
        long remoteReading = 0;
        long totalReading = 0;
        for (int i = 0; i < dfsInputStreams.length; i++) {
          remoteReading += dfsInputStreams[i].getReadStatistics().getRemoteBytesRead();
          totalReading += dfsInputStreams[i].getReadStatistics().getTotalBytesRead();
        }
        // write to a local file
        File file = new File("/tmp/rpp_iostats");
        if (!file.isFile()) {
          file.createNewFile();
        }
        FileOutputStream fileOutputStream = new FileOutputStream(file, true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));

        Date date = new Date();
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd|HH:mm");
        bufferedWriter.write(dateFormat.format(date) + " " + splitId + " " + totalReading / 1024.0 + " " +
            remoteReading / 1024.0 + "\n");
        bufferedWriter.close();

        LOG.info(
            dateFormat.format(date) + " " + splitId + " " + totalReading / 1024.0 + " " +
                remoteReading / 1024.0 + "\n");

        ioStatics.writeTofile();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
