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
import com.cug.rpp4raster2d.util.CellIndexInfo;
import com.cug.rpp4raster2d.util.GroupInfo;
import com.cug.rpp4raster3d.raster3d.CellAttrs;
import com.cug.rpp4raster3d.raster3d.CellAttrsSimple;
import com.cug.rpp4raster2d.util.SpatialConstant;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


// input : EdgeInputSplit
// output : key(left up position of inputSplit)
//          value(width/height/value)

/**
 * 将InputSplit解析成键值对
 * 一个inputSplit只得到一个键值对，其中key为splitId，value为{@link InputSplitWritable}，记录所在splitId的宽，高和具体数据
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Evolving
public class SpatialRecordReaderGroupRaster3D extends RecordReader<LongWritable, InputSplitWritableRaster3D> {
  private static final Log LOG = LogFactory.getLog(SpatialRecordReaderGroupRaster3D.class);

  public int radius = 5; // analysis radius

  private FSDataInputStream[] inputStreams;
  //  private int splitColId;
  //  private int splitRowId;
  private LongWritable key;
  private InputSplitWritableRaster3D value;
  private FileSplitGroupRaster3D inputSplit;

  private int cellXDim;
  private int cellYDim;
  private int cellZDim;
  private int groupXSize; // number of groups in x dimension
  private int groupYSize;
  private int groupZSize;

  private int groupInfoZSize; // zSize of GroupInfo
  // 顶层和底层时 groupZSize=groupInfoSize+1
  // 中间时，groupZSize=groupInfoSize+2

  //  int cellRowSize = 1000;
  //  int cellColSize = 1000;
  //  int groupRowSize = 10;
  //  int groupColSize = 10;
  private boolean isFirstColGroup; // mark the first column group, the overlapped row is also FirstColGroup !
  private boolean isFirstZGroup; // mark the first column group, the overlapped row is also FirstColGroup !
  private int splitId;


  public SpatialRecordReaderGroupRaster3D() {

  }


  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    inputSplit = (FileSplitGroupRaster3D) genericSplit;
    Configuration conf = context.getConfiguration();
    final Path[] paths = inputSplit.getPaths();

    // open the file and seek to the start of the inputSplit
    final FileSystem fs = paths[0].getFileSystem(conf);
    inputStreams = new FSDataInputStream[paths.length];
    for (int i = 0; i < paths.length; i++) {
      inputStreams[i] = fs.open(paths[i]);
    }

    radius = inputSplit.radius;
    cellXDim = inputSplit.cellXDim;
    cellYDim = inputSplit.cellYDim;
    cellZDim = inputSplit.cellZDim;
    groupXSize = inputSplit.groupXSize;
    groupYSize = inputSplit.groupYSize;
    groupZSize = inputSplit.groupZSize;
    //    isFirstColGroup = inputSplit.isFirstColGroup; // mark the first column group, the overlapped row is also
    CellIndexInfo cellIndexInfo = CellIndexInfo.getGridCellInfoFromFilename(paths[0].toString());
    //    assert cellIndexInfo != null;
    isFirstColGroup = cellIndexInfo.colId == 0;

    // z 方向由于需要考虑上下radius内的数据，因此较为特殊，考虑需要传入GroupInfo.zSize

    groupInfoZSize = GroupInfo.getDefaultGroupInfo().zSize;
    isFirstZGroup = cellIndexInfo.zId == 0 && groupZSize == groupInfoZSize + 1;

    splitId = inputSplit.splitId;
  }


  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable(inputSplit.splitId);
      LOG.debug("splitId of currrent RecordReader is : " + key);
    } else {
      return false;
    }

    // the width of first col group is different from other right col group

    int valueXDim;
    int valueYDim;
    int valueZDim;
    if (isFirstColGroup) {
      valueXDim = cellXDim * groupXSize;
    } else {
      valueXDim = (2 * radius) % cellXDim + cellXDim * (groupXSize - 1);
    }

    // TODO 不考虑半径超过一个文件的情况 因此groupZSize在顶层和底层时为2，其它层为3
    if (groupZSize == groupInfoZSize + 1) { // 顶底两层
      valueZDim = cellZDim * (groupZSize - 1) + radius;
    } else {
      valueZDim = cellZDim * (groupZSize - 2) + radius * 2;
    }

    CellAttrsSimple[] dataValue;

    if (splitId >= SpatialConstant.ROW_OVERLAPPED_GROUP_SPLIT_ID_BEGIN) { // overlapped row
      valueYDim = radius * 4;
      dataValue = new CellAttrsSimple[valueXDim * valueYDim * valueZDim];

      for (int zz = 0; zz < groupZSize; zz++) {
        for (int yy = 0; yy < groupYSize; yy++) {
          for (int xx = 0; xx < groupXSize; xx++) {
            int startX;
            int startY;
            int startZ;
            int lengthX;
            int lengthY;
            int lengthZ;
            int toValueX;
            int toValueY;
            int toValueZ;

            if (isFirstZGroup) {
              startZ = 0;
              if (zz == 0) {
                lengthZ = cellZDim;
                toValueZ = 0;
              } else { // (zz == 1)
                lengthZ = radius;
                toValueZ = cellZDim;
              }
            } else if (groupZSize == 2) {
              if (zz == 0) {
                startZ = cellZDim - radius;
                lengthZ = radius;
                toValueZ = 0;
              } else {
                startZ = 0;
                lengthZ = cellZDim;
                toValueZ = radius;
              }
            } else {
              if (zz == 0) {
                startZ = cellZDim - radius;
                lengthZ = radius;
                toValueZ = 0;
              } else if (zz == 1) {
                startZ = 0;
                lengthZ = cellZDim;
                toValueZ = radius;
              } else { // zz == 2
                startZ = 0;
                lengthZ = radius;
                toValueZ = radius + cellZDim;
              }
            }

            if (yy == 0) {
              startY = cellYDim - 2 * radius;
              lengthY = 2 * radius;
              toValueY = 0;
            } else { // yy == 1
              startY = 0;
              lengthY = 2 * radius;
              toValueY = 2 * radius;
            }

            startX = 0;
            lengthX = cellXDim;
            toValueX = xx * cellXDim;
            int cellSize = new CellAttrsSimple().getSize();

            readPartFromStream(inputStreams[zz * groupXSize * groupYSize + yy * groupXSize + xx], cellXDim,
                cellYDim, startX, startY, startZ, lengthX, lengthY, lengthZ,
                cellSize, dataValue, valueXDim, valueYDim,
                toValueX, toValueY, toValueZ);
          }
        }
      }
    } else {  // normal group

      valueYDim = cellYDim * groupYSize;
      dataValue = new CellAttrsSimple[valueXDim * valueYDim * valueZDim];

      for (int zz = 0; zz < groupZSize; zz++) {
        for (int yy = 0; yy < groupYSize; yy++) {
          for (int xx = 0; xx < groupXSize; xx++) {
            int startX;
            int startY;
            int startZ;
            int lengthX;
            int lengthY;
            int lengthZ;
            int toValueX;
            int toValueY;
            int toValueZ;

            if (isFirstZGroup) {
              startZ = 0;
              if (zz == 0) {
                lengthZ = cellZDim;
                toValueZ = 0;
              } else {
                lengthZ = radius;
                toValueZ = cellZDim;
              }
            } else if (groupZSize == 2) { // bottom group of Z
              if (zz == 0) {
                startZ = cellZDim - radius;
                lengthZ = radius;
                toValueZ = 0;
              } else {
                startZ = 0;
                lengthZ = cellZDim;
                toValueZ = radius;
              }
            } else {
              if (zz == 0) {
                startZ = cellZDim - radius;
                lengthZ = radius;
                toValueZ = 0;
              } else if (zz == 1) {
                startZ = 0;
                lengthZ = cellZDim;
                toValueZ = radius;
              } else {
                startZ = 0;
                lengthZ = radius;
                toValueZ = radius + cellZDim;
              }
            }

            if (isFirstColGroup) {
              startX = 0;
              startY = 0;
              lengthX = cellXDim;
              lengthY = cellYDim;
              toValueX = xx * cellXDim;
              toValueY = yy * cellYDim;
            } else {
              startY = 0;
              toValueY = yy * cellYDim;
              lengthY = cellYDim;
              if (xx == 0) {
                startX = cellXDim - 2 * radius;
                lengthX = 2 * radius;
                toValueX = 0;
              } else {
                startX = 0;
                lengthX = cellXDim;
                toValueX = radius * 2 + (xx - 1) * cellXDim;
              }
            }

            int cellSize = new CellAttrsSimple().getSize();
            readPartFromStream(inputStreams[zz * groupXSize * groupYSize + yy * groupXSize + xx], cellXDim,
                cellYDim, startX, startY, startZ, lengthX, lengthY, lengthZ,
                cellSize, dataValue, valueXDim, valueYDim,
                toValueX, toValueY, toValueZ);

          }
        }
      }

    }

    value = new InputSplitWritableRaster3D(new IntWritable(valueXDim), new IntWritable(valueYDim),
        new IntWritable(valueZDim), dataValue);

    return true;

  }

  /**
   * 一个辅助函数
   * 从一个文件（InputStream中读取一部分到目标数组
   * 为了降低前面6重for循环的复杂度
   */
  void readPartFromStream(FSDataInputStream inputStream, int cellXDim, int cellYDim,
                          int startX, int startY, int startZ, int lengthX, int lengthY, int lengthZ,
                          int cellAttrSize,
                          CellAttrsSimple[] value, int valueXDim, int valueYDim,
                          int toValueX, int toValueY, int toValueZ) throws IOException {

    inputStream.skip(cellXDim * cellYDim * startZ * cellAttrSize);
    for (int zz = 0; zz < lengthZ; zz++) {
      inputStream.skip(cellXDim * startY * cellAttrSize);
      for (int yy = 0; yy < lengthY; yy++) {
        inputStream.skip(startX * cellAttrSize);
        for (int xx = 0; xx < lengthX; xx++) {
          value[toValueX + xx +
              (toValueY + yy) * valueXDim
              + (toValueZ + zz) * valueXDim * valueYDim]
              = new CellAttrsSimple(inputStream);
        }
        inputStream.skip((cellXDim - lengthX - startX) * cellAttrSize);
      }
      inputStream.skip((cellYDim - lengthY - startY) * cellXDim * cellAttrSize);
    }
  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public InputSplitWritableRaster3D getCurrentValue() {
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
        long remoteReading = 0;
        long totalReading = 0;
        for (int i = 0; i < inputStreams.length; i++) {
          if(inputStreams[i] instanceof HdfsDataInputStream){
            remoteReading += ((HdfsDataInputStream) inputStreams[i]).getReadStatistics().getRemoteBytesRead();
            totalReading += ((HdfsDataInputStream) inputStreams[i]).getReadStatistics().getTotalBytesRead();
          }
          inputStreams[i].close();
        }

         // write to a local file
        File file = new File("/tmp/rpp_iostats");
        if(!file.isFile())
          file.createNewFile();
        FileOutputStream fileOutputStream = new FileOutputStream(file, true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));

        Date date = new Date();
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd|HH:mm");
        bufferedWriter.write(dateFormat.format(date) + " " + splitId + " " + totalReading/1024 + " " +
            remoteReading/1024 + "\n");
        bufferedWriter.close();

        LOG.info(dateFormat.format(date) + " " + splitId + " " + totalReading/1024 + " " + remoteReading/1024 + "\n");

      }
    } catch (IOException e) {
      e.printStackTrace();
    }


  }
}
