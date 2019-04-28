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
import com.cug.geo3d.util.SpatialConstant;
import org.apache.commons.io.FilenameUtils;
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
import org.apache.hadoop.io.Writable;
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
public class SpatialRecordReaderTradition extends RecordReader<LongWritable, InputSplitWritable> {
  private static final Log LOG = LogFactory.getLog(SpatialRecordReaderTradition.class);

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
  private int groupRowSize = 10;
  private int groupColSize = 10;
  private boolean isFirstColGroup = true; // mark the first column group, the overlapped row is also FirstColGroup !
  private int splitId = 0;

  public SpatialRecordReaderTradition() {
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


    radius = inputSplit.radius;
    cellRowSize = inputSplit.cellRowSize;
    cellColSize = inputSplit.cellColSize;
    groupRowSize = inputSplit.groupRowSize; // useless
    groupColSize = inputSplit.groupColSize; // useless
    isFirstColGroup = inputSplit.isFirstColGroup; // useless
    splitId = inputSplit.splitId;


    //    GridCellInfo leftTop = new GridCellInfo();
    //    GridCellInfo.getGridIndexFromFilename(paths[0].getName(), leftTop);

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
          dataValue[(i+cellRowSize) * valueWidth + j] = new IntWritable(inputStreams[2].readInt());
        }
      }

      for (int i = 0; i < radius * 2; i++) {
        for (int j = 0; j < radius * 2; j++) {
          dataValue[(i+cellRowSize) * valueWidth + j + cellColSize] = new IntWritable(inputStreams[3].readInt());
        }
      }
    }

    if (paths.length == 2){
      GridCellInfo cell1 = GridCellInfo.getGridIndexFromFilename(paths[0].toString());
      GridCellInfo cell2 = GridCellInfo.getGridIndexFromFilename(paths[1].toString());
      if(cell1.rowId == cell2.rowId){ // bottom most row
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
      } else{ // right most column
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
    }



    if (isFirstColGroup) {
      valueWidth = cellColSize + radius * 2;
      valueHeight = cellColSize + radius * 2;
    } else {
      valueWidth = cellColSize * (groupColSize - 1) + (2 * radius) % cellColSize;
    }

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
                    valueWidth * (ii + ((2 * radius) % cellRowSize + (i - 1) * cellRowSize))
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
                inputStreams[i * groupColSize + j].skip((cellColSize - (radius * 2) % cellColSize) * 4);
                for (int jj = 0; jj < (radius * 2) % cellColSize; jj++) {
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

    value = new

        InputSplitWritable(new IntWritable(valueWidth), new

        IntWritable(valueHeight), dataValue);

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
