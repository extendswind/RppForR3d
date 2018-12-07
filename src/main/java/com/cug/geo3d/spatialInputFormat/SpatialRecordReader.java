package com.cug.geo3d.spatialInputFormat; /**
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

import com.cug.geo3d.util.GridCellInfo;
import com.cug.geo3d.util.GridIndexInfo;
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
 * Treats keys as offset in file and value as line.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Evolving
public class SpatialRecordReader extends RecordReader<LongWritable, InputSplitWritable> {
  private static final Log LOG = LogFactory.getLog(SpatialRecordReader.class);
  public static final String MAX_LINE_LENGTH =
          "mapreduce.input.linerecordreader.line.maxlength";

  public int radius = 5; // TODO 从哪传入 set to default five

  private FSDataInputStream[] inputStreams;
  private int splitColId;
  private int splitRowId;
  private LongWritable key;
  private InputSplitWritable value;
  private EdgeFileSplit inputSplit;

  public SpatialRecordReader() throws IOException {
    inputStreams = new FSDataInputStream[4];

  }


  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    inputSplit = (EdgeFileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    final Path[] paths = inputSplit.getPaths();


    // open the file and seek to the start of the inputSplit
    final FileSystem fs = paths[0].getFileSystem(job);
    for (int i = 0; i < paths.length; i++) {
      inputStreams[i] = fs.open(paths[i]);
    }

    GridCellInfo leftTop = new GridCellInfo();
    GridCellInfo.getGridIndexFromFilename(paths[0].getName(), leftTop);
    splitColId = leftTop.colId;
    splitRowId = leftTop.rowId;

    radius = context.getConfiguration().getInt("geo3d.spatial.radius", 5);

  }


  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable(inputSplit.getSplitId());
    } else {
      return false;
    }

    GridIndexInfo gridIndexInfo = inputSplit.getGridIndexInfo();

    int startRow = 0;
    int startCol = 0;
    int endRow = gridIndexInfo.cellRowSize;
    int endCol = gridIndexInfo.cellColSize;
    if (splitRowId != 0)
      startRow = gridIndexInfo.cellRowSize / 2 - radius;
    if (splitColId != 0)
      startCol = gridIndexInfo.cellColSize / 2 - radius;
    if (splitRowId != gridIndexInfo.gridRowSize - 2)
      endRow = gridIndexInfo.cellRowSize / 2 + radius;
    if (splitColId != gridIndexInfo.cellColSize - 2)
      endCol = gridIndexInfo.cellColSize / 2 + radius;

    int valueWidth = gridIndexInfo.cellColSize - startCol + endCol;
    int valueHeight = gridIndexInfo.cellRowSize - startRow + endRow;

    IntWritable[] dataValue = new IntWritable[valueHeight * valueWidth];


    // left-top file
    for (int row = 0; row < gridIndexInfo.cellRowSize - startRow; row++) {
      inputStreams[0].skip(startCol);
      for (int col = 0; col < gridIndexInfo.cellColSize - startCol; col++) {
        dataValue[row * valueWidth + col] = new IntWritable(inputStreams[0].readInt());
      }
    }

    // right-top file
    for (int row = 0; row < gridIndexInfo.cellRowSize - startRow; row++) {
      for (int col = gridIndexInfo.cellColSize - startCol; col < valueWidth; col++) {
        dataValue[row * valueWidth + col] = new IntWritable(inputStreams[1].readInt());
      }
      inputStreams[1].skip(gridIndexInfo.cellColSize - endCol);
    }

    // left-bottom file
    for (int row = gridIndexInfo.cellRowSize - startRow; row < valueHeight; row++) {
      inputStreams[2].skip(startCol);
      for (int col = 0; col < gridIndexInfo.cellColSize - startCol; col++) {
        dataValue[row * valueWidth + col] = new IntWritable(inputStreams[2].readInt());
      }
    }

    // right-bottom file
    for (int row = gridIndexInfo.cellRowSize - startRow; row < valueHeight; row++) {
      for (int col = gridIndexInfo.cellColSize - startCol; col < valueWidth; col++) {
        dataValue[row * valueWidth + col] = new IntWritable(inputStreams[3].readInt());
      }
      inputStreams[3].skip(gridIndexInfo.cellColSize - endCol);
    }

    value = new InputSplitWritable(
            new IntWritable(valueWidth), new IntWritable(valueHeight), dataValue);

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
  public float getProgress() throws IOException {
    if (key == null)
      return 0.0f;
    else
      return 1.0f;
  }

  @Override
  public synchronized void close() throws IOException {
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
