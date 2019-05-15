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

import com.cug.geo3d.util.SpatialConstant;
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
public class SpatialRecordReaderGroup extends RecordReader<LongWritable, InputSplitWritable> {
  private static final Log LOG = LogFactory.getLog(SpatialRecordReaderGroup.class);

  public int radius = 5; // analysis radius

  private FSDataInputStream[] inputStreams;
  //  private int splitColId;
//  private int splitRowId;
  private LongWritable key;
  private InputSplitWritable value;
  private FileSplitGroup inputSplit;


  int cellRowSize = 1000;
  int cellColSize = 1000;
  int groupRowSize = 10;
  int groupColSize = 10;
  boolean isFirstColGroup = true; // mark the first column group, the overlapped row is also FirstColGroup !
  int splitId = 0;

  public SpatialRecordReaderGroup() {
  }


  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    inputSplit = (FileSplitGroup) genericSplit;
    Configuration conf = context.getConfiguration();
    final Path[] paths = inputSplit.getPaths();


    // open the file and seek to the start of the inputSplit
    final FileSystem fs = paths[0].getFileSystem(conf);
    inputStreams = new FSDataInputStream[paths.length];
    for (int i = 0; i < paths.length; i++) {
      inputStreams[i] = fs.open(paths[i]);
    }


    radius = inputSplit.radius;
    cellRowSize = inputSplit.cellRowSize;
    cellColSize = inputSplit.cellColSize;
    groupRowSize = inputSplit.groupRowSize;
    groupColSize = inputSplit.groupColSize;
    isFirstColGroup = inputSplit.isFirstColGroup; // mark the first column group, the overlapped row is also
    splitId = inputSplit.splitId;


//    GridCellInfo leftTop = new GridCellInfo();
//    GridCellInfo.getGridCellInfoFromFilename(paths[0].getName(), leftTop);

//    radius = context.getConfiguration().getInt("geo3d.spatial.radius", 5);

  }


  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable(inputSplit.splitId);
    } else {
      return false;
    }

    // the width of first col group is different from other right col group

    int valueWidth;
    int valueHeight;
    if (isFirstColGroup) {
      valueWidth = cellColSize * groupColSize;
    } else {
      valueWidth = cellColSize * (groupColSize - 1) + (2 * radius)%cellColSize;
    }
    IntWritable[] dataValue ;

    if (splitId >= SpatialConstant.ROW_OVERLAPPED_GROUP_SPLIT_ID_BEGIN) { // overlapped row
      valueHeight = radius * 4;
      dataValue = new IntWritable[valueHeight * valueWidth];


      for (int i = 0; i < groupRowSize; i++) {
        for (int j = 0; j < groupColSize; j++) {  // for top rows
          if (i == 0) {  // 第一行，need skip the rows which is far than radius
            inputStreams[i * groupColSize + j].skip(cellColSize * (cellRowSize - ((2 * radius) % cellRowSize)) * 4);
            for (int ii = 0; ii < (2 * radius) % cellRowSize; ii++) {
              for (int jj = 0; jj < cellColSize; jj++) {  // for every value in a file
                dataValue[cellColSize * j + jj + // col number of the value
                    valueWidth * ii   // width * (row number of the value)
                    ] = new IntWritable(inputStreams[i * groupColSize + j].readInt());
              }
            }
          } else if (i == groupRowSize - 1) { // last row
            for (int ii = 0; ii < (2 * radius) % cellRowSize; ii++) {
              for (int jj = 0; jj < cellColSize; jj++) {  // for every value in a file
                dataValue[cellColSize * j + jj + // col number of the value
                    valueWidth * (ii + ((2 * radius) % cellRowSize + (i - 1) * cellRowSize))
                    // width * (row number of  the value)
                    ] = new IntWritable(inputStreams[i * groupColSize + j].readInt());
              }
            }
          } else {
            for (int ii = 0; ii < cellRowSize; ii++) {
              for (int jj = 0; jj < cellColSize; jj++) {  // for every value in a file
                dataValue[cellColSize * j + jj + // col number of the value
                    valueWidth * (ii + ((2*radius) % cellRowSize + (i - 1) * cellRowSize))
                    // width * (row number of the value)
                    ] = new IntWritable(inputStreams[i * groupColSize + j].readInt());
              }
            }
          }
        }
      }
    } else {  // normal group

      valueHeight = cellRowSize * groupRowSize;
      dataValue = new IntWritable[valueHeight * valueWidth];
      for (int i = 0; i < groupRowSize; i++) {
        for (int j = 0; j < groupColSize; j++) {  // for every file

          if (isFirstColGroup) {
            for (int ii = 0; ii < cellRowSize; ii++) {
              for (int jj = 0; jj < cellColSize; jj++) {  // for every value in a file
                dataValue[cellColSize * j + jj + // col number of the value
                    valueWidth * (ii + cellRowSize * i)  // width * (row number of the value)
                    ] = new IntWritable(inputStreams[i * groupColSize + j].readInt());
              }
            }
          } else {
            if (j == 0) {
              for (int ii = 0; ii < cellRowSize; ii++) {
                inputStreams[i * groupColSize + j].skip((cellColSize - (radius * 2)%cellColSize) * 4);
                for (int jj = 0; jj < (radius * 2)%cellColSize; jj++) {
                  dataValue[jj + // col number of the value
                      valueWidth * (ii + cellRowSize * i)  // width * (row number of the value)
                      ] = new IntWritable(inputStreams[i * groupColSize + j].readInt());
                }
              }
            } else {
              for (int ii = 0; ii < cellRowSize; ii++) {
                for (int jj = 0; jj < cellColSize; jj++) {
                  dataValue[jj + cellColSize * (j - 1) + (radius * 2) % cellColSize + // col number of the value
                      valueWidth * (ii + cellRowSize * i)  // width * (row number of the value)
                      ] = new IntWritable(inputStreams[i * groupColSize + j].readInt());
                }
              }
            }
          }
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
    if (key == null) return 0.0f;
    else return 1.0f;
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
