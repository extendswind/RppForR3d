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
import com.cug.rpp4raster3d.raster3d.CellAttrsSimple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;


// input : EdgeInputSplit
// output : key(left up position of inputSplit)
//          value(width/height/value)

/**
 * 将InputSplit解析成键值对
 * 一个inputSplit只得到一个键值对，其中key为splitId，value为{@link InputSplitWritable}，记录所在splitId的宽，高和具体数据
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Evolving
public class SpatialRecordReaderSimpleRaster3D extends RecordReader<LongWritable, InputSplitWritableRaster3D> {
  private static final Log LOG = LogFactory.getLog(SpatialRecordReaderSimpleRaster3D.class);

  public int radius; // analysis radius

  private FSDataInputStream[] inputStreams;
  private LongWritable key;
  private InputSplitWritableRaster3D value;
  private FileSplitGroupRaster3D inputSplit;
  private Configuration conf;

  private int cellXDim;
  private int cellYDim;
  private int cellZDim;
  private int groupXSize; // number of groups in x dimension
  private int groupYSize;
  private int groupZSize;
  CellIndexInfo cellIndexInfo;

  private int splitId;


  public SpatialRecordReaderSimpleRaster3D() {

  }


  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    inputSplit = (FileSplitGroupRaster3D) genericSplit;
    conf = context.getConfiguration();
    final Path[] paths = inputSplit.getPaths();

    // open the file and seek to the start of the inputSplit
    FileSystem fs = paths[0].getFileSystem(conf);
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
    cellIndexInfo = CellIndexInfo.getGridCellInfoFromFilename(paths[0].toString());
    //    assert cellIndexInfo != null;

    splitId = inputSplit.splitId;
  }


  public boolean nextKeyValue() {
    if (key == null) {
      key = new LongWritable(inputSplit.splitId);
    } else {
      return false;
    }

    int valueXDim = (groupXSize - 1) * radius + cellXDim;
    int valueYDim = (groupYSize - 1) * radius + cellYDim;
    int valueZDim = (groupZSize - 1) * radius + cellZDim;

    CellAttrsSimple[] dataValue = new CellAttrsSimple[valueXDim * valueYDim * valueZDim];

    int groupXEnd = 1;
    int groupYEnd = 1;
    int groupZEnd = 1;

    int[] groupStart = new int[]{-1, -1, -1};

    if(groupXSize == 2){
      if(cellIndexInfo.colId == 0)
        groupStart[0] = 0;
      else
        groupXEnd = 0;
    }

    if(groupYSize == 2){
      if(cellIndexInfo.rowId == 0)
        groupStart[1] = 0;
      else
        groupYEnd = 0;
    }

    if(groupZSize == 2){
      if(cellIndexInfo.zId == 0)
        groupStart[2] = 0;
      else
        groupZEnd = 0;
    }

    int[] startPos = new int[3]; // startX startY startZ   reading start pos
    int[] lengths = new int[3]; // lengthX lengthY lengthZ  reading length
    int[] toValuePos = new int[3]; // toValueX toValueY toValueZ    position in value to save reading data

    int[] filePos = new int[3];
    int[] cellDims = new int[]{cellXDim, cellYDim, cellZDim};
    for (filePos[2] = groupStart[2]; filePos[2]<= groupZEnd; filePos[2]++) {
      for (filePos[1] = groupStart[1]; filePos[1] <= groupYEnd; filePos[1]++) {
        for (filePos[0] = groupStart[0]; filePos[0] <= groupXEnd; filePos[0]++) {

          // x y z
          for(int i=0; i<3; i++){
            if(filePos[i] == -1){
              startPos[i] = cellDims[i] - radius;
              lengths[i] = radius;
              toValuePos[i] = 0;
            } else if(filePos[i] == 0){
              startPos[i] = 0;
              lengths[i] = cellDims[i];
              if(groupStart[i] == -1)
                toValuePos[i] = radius;
              else
                toValuePos[i] = 0;
            } else if(filePos[i] == 1){
              startPos[i] = 0;
              lengths[i] = radius;
              if(groupStart[i] == -1)
                toValuePos[i] = radius + cellDims[i];
              else
                toValuePos[i] = cellDims[i];
            }
          }

          int cellSize = new CellAttrsSimple().getSize();
          try {
            readPartFromStream(inputStreams[(filePos[2] - groupStart[2]) * groupXSize * groupYSize +
                    (filePos[1] - groupStart[1]) * groupXSize + (filePos[0] - groupStart[0])],
                cellXDim, cellYDim, startPos[0], startPos[1], startPos[2], lengths[0], lengths[1], lengths[2],
                cellSize, dataValue, valueXDim, valueYDim, toValuePos[0], toValuePos[1], toValuePos[2]);
          } catch (IOException e) {
//            e.printStackTrace();
            System.err.println("File Reading Error!!!");
            LOG.error("file reading error! key: " + splitId + ", error file number: " +
                ((filePos[2] - groupStart[2]) * groupXSize * groupYSize +
                    (filePos[1] - groupStart[1]) * groupXSize + (filePos[0] - groupStart[0])));
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
   * 从一个文件（InputStream）中读取一部分到目标数组
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

        // 由于HDFS的文件不能同时写，导致部分数据写入失败，因此还是本地文件吧...
        //        System.setProperty("HADOOP_USER_NAME", "sparkl");
//        Path statsFilePath =new Path("hdfs://kvmmaster:9000/tmp/rpp_iostats");
//        FileSystem fs = FileSystem.get(new URI("hdfs://kvmmaster:9000/tmp/rpp_iostats"), conf);
//        FSDataOutputStream fileOutputStream;
//        if(fs.exists(statsFilePath)){
//          fileOutputStream = fs.append(statsFilePath);
//        } else{
//          fileOutputStream = fs.create(statsFilePath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL),
//              false, conf.getInt( CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
//                  CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT), (short) 3,
//              conf.getLong("fs.local.block.size", 128 * 1024 * 1024), null);
//        }
//        Date data = new Date();
//        fileOutputStream.write((data.getTime() + " " + splitId + " " + totalReading/1024/1024 + " " +
//            remoteReading/1024/1024 + "\n").getBytes(StandardCharsets.UTF_8));
//        fileOutputStream.close();



         // write to a local file
        File file = new File("/tmp/rpp_iostats");
        if(!file.isFile())
          file.createNewFile();
        FileOutputStream fileOutputStream = new FileOutputStream(file, true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));

        Date date = new Date();
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd|HH:mm");
        bufferedWriter.write(dateFormat.format(date) + " " + splitId + " " + totalReading/1024.0/1024.0 + " " +
            remoteReading/1024.0/1024.0 + "\n");
        bufferedWriter.close();

        LOG.info(dateFormat.format(date) + " " + splitId + " " + totalReading/1024 + " " + remoteReading/1024 + "\n");

      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
