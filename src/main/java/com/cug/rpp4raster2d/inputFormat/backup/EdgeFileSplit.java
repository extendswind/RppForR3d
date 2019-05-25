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

package com.cug.rpp4raster2d.inputFormat.backup;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** A section of an input file.  Returned by {@link
 * InputFormat#getSplits(JobContext)} and passed to
 *
 * Implemention of InputSplit should also implement Writable for spark reading!!!!!!!!!!!
 *
 * {@link InputFormat#createRecordReader(InputSplit, TaskAttemptContext)}. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class EdgeFileSplit extends InputSplit implements Writable {
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
  public EdgeFileSplit(Path[] files, long length, String[] hosts, int splitId, int cellRowNum, int cellColNum,
                       int cellRowSize, int cellColSize) {
    this.files = files;
    this.length = length;
    this.hosts = hosts;
    this.splitId = splitId;
    gridIndexInfo = new GridIndexInfo(cellRowNum, cellColNum, cellRowSize, cellColSize);
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

  /**
   * Serialize the fields of this object to <code>out</code>.
   *
   * @param out <code>DataOuput</code> to serialize this object into.
   * @throws IOException
   */
  @Override
  public void write(DataOutput out) throws IOException {
    // files
    out.writeInt(files.length);
    for (Path p: files ) {
      Text.writeString(out, p.toString());
    }

    out.writeLong(length); // length

    out.writeInt(hosts.length); // hosts
    for(String s : hosts){
      Text.writeString(out, s);
    }

    out.writeInt(splitId); // splitId

    // gridIndexInfo
    out.writeInt(gridIndexInfo.cellRowNum);
    out.writeInt(gridIndexInfo.cellColNum);
    out.writeInt(gridIndexInfo.cellRowSize);
    out.writeInt(gridIndexInfo.cellColSize);
  }

  /**
   * Deserialize the fields of this object from <code>in</code>.
   * <p>
   * <p>For efficiency, implementations should attempt to re-use storage in the
   * existing object where possible.</p>
   *
   * @param in <code>DataInput</code> to deseriablize this object from.
   * @throws IOException
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    // files
    files = new Path[in.readInt()];
    for(int i=0; i<files.length; i++){
      files[i] = new Path(Text.readString(in));
    }

    length = in.readLong();

    hosts = new String[in.readInt()];
    for(int i=0; i<hosts.length; i++){
      hosts[i] = Text.readString(in);
    }

    splitId = in.readInt();

    gridIndexInfo = new GridIndexInfo(in.readInt(), in.readInt(), in.readInt(), in.readInt());
  }

//  default value return null, represent no memory location considered
// @Override
//  @Evolving
//  public SplitLocationInfo[] getLocationInfo() throws IOException {
//    return hostInfos;
//  }
}
