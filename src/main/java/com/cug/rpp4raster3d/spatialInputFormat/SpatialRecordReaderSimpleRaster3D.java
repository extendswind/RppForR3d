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
import com.cug.rpp4raster3d.raster3d.NormalRaster3D;
import com.cug.rpp4raster3d.raster3d.Raster3D;
import com.cug.rpp4raster3d.raster3d.SimpleRaster3D;
import com.cug.rpp4raster3d.raster3dGenerate.Raster3dGeneratorAndUploader;
import com.google.gson.annotations.Since;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StopWatch;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.hadoop.fs.FileSystem.DEFAULT_FS;
import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;


// input : EdgeInputSplit
// output : key(left up position of inputSplit)
//          value(width/height/value)

/**
 * 将InputSplit解析成键值对
 * 一个inputSplit只得到一个键值对，其中key为splitId，value为{@link InputSplitWritable}，记录所在splitId的宽，高和具体数据
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Evolving
@Since(1.8)
public class SpatialRecordReaderSimpleRaster3D extends RecordReader<LongWritable, Raster3D> {
  private static final Log LOG = LogFactory.getLog(SpatialRecordReaderSimpleRaster3D.class);

  public int radius; // analysis radius

  private FSDataInputStream[] inputStreams;  // for reading data
  private DFSInputStream[] dfsInputStreams; //  for reading statistics
  private LongWritable key;
  private Raster3D value;
  //  private InputSplitWritableRaster3D value;
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
    dfsInputStreams = new DFSInputStream[paths.length];
    inputStreams = new FSDataInputStream[paths.length];

    if (conf.get(FS_DEFAULT_NAME_KEY, DEFAULT_FS).equals(DEFAULT_FS)) {  // reading local files
      FileSystem fs = paths[0].getFileSystem(conf);
      for (int i = 0; i < paths.length; i++) {
        inputStreams[i] = fs.open(paths[i]);
      }
    } else {  // reading dfs files
      DFSClient dfsClient = new DFSClient(NameNode.getAddress(conf), conf);
      for (int i = 0; i < paths.length; i++) {
        dfsInputStreams[i] = dfsClient.open(paths[i].toUri().getRawPath());
//        inputStreams[i] = new FSDataInputStream(new BufferedFSInputStream(dfsInputStreams[i], 40000));
        inputStreams[i] = new FSDataInputStream(dfsInputStreams[i]);
      }
    }

    //        Stream<Path> stream = Arrays.stream(paths);
    //    inputStreams = stream.parallel().map(is -> {
    //      try {
    //        return fs.open(is);
    //      } catch (IOException e) {
    //        e.printStackTrace();
    //        return null;
    //      }
    //    }).toArray(FSDataInputStream[]::new);

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


    StopWatch sw = new StopWatch().start();


    
    int valueXDim = (groupXSize - 1) * radius + cellXDim;
    int valueYDim = (groupYSize - 1) * radius + cellYDim;
    int valueZDim = (groupZSize - 1) * radius + cellZDim;

    //    CellAttrsSimple[] dataValue = new CellAttrsSimple[valueXDim * valueYDim * valueZDim];

    // Raster3D
    String r3dName = conf.get("r3d.class.name", "com.cug.rpp4raster3d.raster3d.NormalRaster3D");
    Raster3D raster3D = null;
    try {
      Constructor constructor = Class.forName(r3dName).getConstructor(new Class[]{int.class, int.class, int.class});
      raster3D = (Raster3D) constructor.newInstance(valueXDim, valueYDim, valueZDim);
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    }

    //    Raster3D raster3D = new NormalRaster3D(valueXDim, valueYDim, valueZDim);

    int groupXEnd = 1;
    int groupYEnd = 1;
    int groupZEnd = 1;

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


    for (filePos[2] = groupStart[2]; filePos[2] <= groupZEnd; filePos[2]++) {
      for (filePos[1] = groupStart[1]; filePos[1] <= groupYEnd; filePos[1]++) {
        for (filePos[0] = groupStart[0]; filePos[0] <= groupXEnd; filePos[0]++) {

          // x y z
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

          int cellSize = raster3D.getCellSize();
          try {
            readPartFromStream(inputStreams[(filePos[2] - groupStart[2]) * groupXSize * groupYSize +
                    (filePos[1] - groupStart[1]) * groupXSize + (filePos[0] - groupStart[0])],
                cellXDim, cellYDim, startPos[0], startPos[1], startPos[2], lengths[0], lengths[1], lengths[2],
                cellSize, raster3D, valueXDim, valueYDim, toValuePos[0], toValuePos[1], toValuePos[2]);
          } catch (IOException e) {
            //            e.printStackTrace();
            //            System.err.println("File Reading Error!!!");
            LOG.error("file reading error! key: " + splitId + ", error file number: " +
                ((filePos[2] - groupStart[2]) * groupXSize * groupYSize +
                    (filePos[1] - groupStart[1]) * groupXSize + (filePos[0] - groupStart[0])));
          }
        }
      }

    }

    value = raster3D;
    //    new InputSplitWritableRaster3D(new IntWritable(valueXDim), new IntWritable(valueYDim), new IntWritable
    //    (valueZDim), );


    sw.stop();
    LOG.debug("data reading time of RecordReader lsakdjfl is " + sw.now(TimeUnit.SECONDS));

    if (LOG instanceof Log4JLogger) {
      LOG.debug(((Log4JLogger) LOG).getLogger().getAllAppenders().toString());
    }


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
                          Raster3D raster3D, int valueXDim, int valueYDim,
                          int toValueX, int toValueY, int toValueZ) throws IOException {

    inputStream.skip(cellXDim * cellYDim * startZ * cellAttrSize);
    for (int zz = 0; zz < lengthZ; zz++) {
      inputStream.skip(cellXDim * startY * cellAttrSize);
      for (int yy = 0; yy < lengthY; yy++) {
        inputStream.skip(startX * cellAttrSize);
        for (int xx = 0; xx < lengthX; xx++) {
          raster3D.readAttr(toValueX + xx +
                  (toValueY + yy) * valueXDim
                  + (toValueZ + zz) * valueXDim * valueYDim
              , inputStream);
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
        long remoteReading = 0;
        long totalReading = 0;
        for (int i = 0; i < inputStreams.length; i++) {
          if (dfsInputStreams[i] != null) {
            remoteReading += dfsInputStreams[i].getReadStatistics().getRemoteBytesRead();
            totalReading += dfsInputStreams[i].getReadStatistics().getTotalBytesRead();
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
        //          fileOutputStream = fs.create(statsFilePath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction
        //          .ALL),
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
        if (!file.isFile()) {
          file.createNewFile();
        }
        FileOutputStream fileOutputStream = new FileOutputStream(file, true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));

        Date date = new Date();
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd|HH:mm");
        bufferedWriter.write(dateFormat.format(date) + " " + splitId + " " + totalReading / 1024.0 / 1024.0 + " " +
            remoteReading / 1024.0 / 1024.0 + "\n");
        bufferedWriter.close();

        LOG.info(
            dateFormat.format(date) + " " + splitId + " " + totalReading / 1024.0 / 1024.0 + " " +
                remoteReading / 1024.0 / 1024.0 + "\n");

      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
