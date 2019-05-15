package com.cug.geo3d.spatialInputFormat;

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

import com.cug.geo3d.util.GridCellInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


// input : EdgeInputSplit
// output : key(left up position of inputSplit)
//          value(width/height/value)

/**
 * 将InputSplit解析成键值对
 * 一个inputSplit只得到一个键值对，其中key为splitId，value为{@link InputSplitWritable}，记录所在splitId的宽，高和具体数据
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Evolving
public class SpatialRecordReaderOrdinary extends RecordReader<LongWritable, InputSplitWritable> {
  private static final Log LOG = LogFactory.getLog(SpatialRecordReaderOrdinary.class);

  public int radius = 5; // analysis radius

  private FSDataInputStream[] inputStreams;
  //  private int splitColId;
  //  private int splitRowId;
  private LongWritable key;
  private InputSplitWritable value;
  private FileSplitGroup inputSplit;
  private Path[] paths;

  private int cellRowSize = 1000;
  private int cellColSize = 1000;

//  private int groupRowSize = 10;
//  private int groupColSize = 10;
//  private boolean isFirstColGroup = true; // mark the first column group, the overlapped row is also FirstColGroup !
  private int splitId = 0;

  public SpatialRecordReaderOrdinary() {
  }


  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    inputSplit = (FileSplitGroup) genericSplit;
    Configuration conf = context.getConfiguration();
    paths = inputSplit.getPaths();

    // open the file and seek to the start of the inputSplit
    final FileSystem fs = paths[0].getFileSystem(conf);
    inputStreams = new FSDataInputStream[paths.length];
    for (int i = 0; i < paths.length; i++) {
      inputStreams[i] = fs.open(paths[i]);
    }

    // the FileSplitGroup is used for SpatialRecordReader
    // therefore some information is not used in this record reader
    radius = inputSplit.radius;
    cellRowSize = inputSplit.cellRowSize;
    cellColSize = inputSplit.cellColSize;
//    groupRowSize = inputSplit.groupRowSize; // useless
//    groupColSize = inputSplit.groupColSize; // useless
//    isFirstColGroup = inputSplit.isFirstColGroup; // useless
    splitId = inputSplit.splitId;
  }


  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable(splitId);
    } else {
      return false;
    }

    int valueWidth;
    int valueHeight;
    IntWritable[] dataValue;

    if (paths.length == 4) {
      valueWidth = cellColSize + radius * 2;
      valueHeight = cellRowSize + radius * 2;
      dataValue = new IntWritable[valueWidth * valueHeight];

      for (int i = 0; i < cellRowSize; i++) {
        for (int j = 0; j < cellColSize; j++) {
          dataValue[i * valueWidth + j] = new IntWritable(inputStreams[0].readInt());
        }
      }

      for (int i = 0; i < cellRowSize; i++) {
        for (int j = 0; j < radius * 2; j++) {
          dataValue[i * valueWidth + j + cellColSize] = new IntWritable(inputStreams[1].readInt());
        }
        inputStreams[1].skip(4 * (cellColSize - radius * 2));
      }

      for (int i = 0; i < radius * 2; i++) {
        for (int j = 0; j < cellColSize; j++) {
          dataValue[(i + cellRowSize) * valueWidth + j] = new IntWritable(inputStreams[2].readInt());
        }
      }

      for (int i = 0; i < radius * 2; i++) {
        for (int j = 0; j < radius * 2; j++) {
          dataValue[(i + cellRowSize) * valueWidth + j + cellColSize] = new IntWritable(inputStreams[3].readInt());
        }
      }
    } else if (paths.length == 2) {
      GridCellInfo cell1 = GridCellInfo.getGridCellInfoFromFilename(paths[0].toString());
      GridCellInfo cell2 = GridCellInfo.getGridCellInfoFromFilename(paths[1].toString());
      if (cell1.rowId == cell2.rowId) { // bottom most row
        valueWidth = cellColSize + radius * 2;
        valueHeight = cellRowSize;
        dataValue = new IntWritable[valueWidth * valueHeight];

        for (int i = 0; i < cellRowSize; i++) {
          for (int j = 0; j < cellColSize; j++) {
            dataValue[i * valueWidth + j] = new IntWritable(inputStreams[0].readInt());
          }
        }

        for (int i = 0; i < cellRowSize; i++) {
          for (int j = 0; j < radius * 2; j++) {
            dataValue[i * valueWidth + j + cellColSize] = new IntWritable(inputStreams[1].readInt());
          }
          inputStreams[1].skip(4 * (cellColSize - radius * 2));
        }
      } else { // right most column
        valueWidth = cellColSize + radius * 2;
        valueHeight = cellRowSize;
        dataValue = new IntWritable[valueWidth * valueHeight];

        for (int i = 0; i < cellRowSize; i++) {
          for (int j = 0; j < cellColSize; j++) {
            dataValue[i * valueWidth + j] = new IntWritable(inputStreams[0].readInt());
          }
        }

        for (int i = 0; i < cellRowSize; i++) {
          for (int j = 0; j < radius * 2; j++) {
            dataValue[i * valueWidth + j + cellColSize] = new IntWritable(inputStreams[1].readInt());
          }
          inputStreams[1].skip(4 * (cellColSize - radius * 2));
        }
      }
    } else { // path.length == 1   bottom-right cell
      valueWidth = cellColSize;
      valueHeight = cellRowSize;
      dataValue = new IntWritable[valueWidth * valueHeight];
      for (int i = 0; i < cellRowSize; i++) {
        for (int j = 0; j < cellColSize; j++) {
          dataValue[i * valueWidth + j] = new IntWritable(inputStreams[0].readInt());
        }
      }
    }

    value = new InputSplitWritable(new IntWritable(valueWidth), new IntWritable(valueHeight), dataValue);
    return true;

  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public InputSplitWritable getCurrentValue() {
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
      if (inputStreams != null) {
        for (int i = 0; i < inputStreams.length; i++) {
          inputStreams[i].close();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
