/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cug.geo3d.spatialInputFormat;

import com.cug.geo3d.util.GridIndexInfo;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/** A section of an input file.  Returned by {@link
 * InputFormat#getSplits(JobContext)} and passed to
 * {@link InputFormat#createRecordReader(InputSplit, TaskAttemptContext)}. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class EdgeFileSplit extends InputSplit {
  private Path []files;  // 左上/右上/左下/右下 四个位置的文件
  private long length;
  private String[] hosts;  // 暂时只传出所有文件都在的host node

  private int splitId; // colId + rowId * width

  private GridIndexInfo gridIndexInfo;

//  private SplitLocationInfo[] hostInfos;

  public EdgeFileSplit() {}

  /** Constructs a split with host information
   *
   * @param files the file name
//   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process
   * @param hosts the list of hosts containing the block, possibly null
   */
  public EdgeFileSplit(Path[] files, long length, String[] hosts, int splitId, int gridRowSize, int gridColSize,
                       int cellRowSize, int cellColSize) {
    this.files = files;
    this.length = length;
    this.hosts = hosts;
    this.splitId = splitId;
    gridIndexInfo = new GridIndexInfo(gridRowSize, gridColSize, cellRowSize, cellColSize);
  }

  /** The file containing this split's data. */
  public Path[] getPaths() { return files; }
  
  /** The number of bytes in the file to process. */
  @Override
  public long getLength() { return length; }

  @Override
  public String[] getLocations() throws IOException {
    if (this.hosts == null) {
      return new String[]{};
    } else {
      return this.hosts;
    }
  }


  public int getSplitId() {
    return splitId;
  }

  public GridIndexInfo getGridIndexInfo() {
    return gridIndexInfo;
  }

//  default value return null, represent no memory location considered
// @Override
//  @Evolving
//  public SplitLocationInfo[] getLocationInfo() throws IOException {
//    return hostInfos;
//  }
}
