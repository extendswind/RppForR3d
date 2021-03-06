/**
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

package com.cug.rpp4raster3d.spatialInputFormat;

import com.cug.rpp4raster2d.inputFormat.FileSplitGroup;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A section of an input file.  Returned by {@link
 * InputFormat#getSplits(JobContext)} and passed to
 * <p>
 * Implemention of InputSplit should also implement Writable for spark reading!!!!!!!!!!!
 * <p>
 * {@link InputFormat#createRecordReader(InputSplit, TaskAttemptContext)}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileSplitGroupRaster3D extends InputSplit implements Writable {

  private Path[] files;  // 一个Group中的所有文件
  private String[] hosts;  // 暂时只传出所有文件都在的host node
  private long length; // 由于数据较为规则，length 直接给 1 不作考虑
  // if not overlapped row group: groupColId + groupRowId * groupColSize
  // if overlapped row group: groupRowId + 100000  TODO  better to write another writable for key
  // splitId 可以得到空间位置

  //  private GridIndexInfo gridIndexInfo; // 读文件时用
//  private GroupCellInfo groupCellInfo;

  public int splitId; //  will be key of a key-value pair
  public int radius;
  public int cellXDim;
  public int cellYDim;
  public int cellZDim;
  public int groupXSize;
  public int groupYSize;
  public int groupZSize;
  public boolean isFristZGroup;

//  public boolean isFirstColGroup; // mark the first column group, the overlapped row is also FirstColGroup


//  private SplitLocationInfo[] hostInfos;

  public FileSplitGroupRaster3D() {
  }


  /**
   *  used for simple SpatialRecordReaderRaster3DSimple
   *  isFirstZGroup is not necessary and is assigned false
   */
  public FileSplitGroupRaster3D(Path[] files, long length, String[] hosts, int splitId, int cellXDim, int cellYDim,
                                int cellZDim, int groupXSize, int groupYSize, int groupZSize, int radius) {
    this(files, length, hosts, splitId, cellXDim, cellYDim, cellZDim, groupXSize, groupYSize, groupZSize, false,
        radius);
  }


  /**
   * Constructs a split with host information
   *
   * @param files  the file name
   *               //   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process
   * @param hosts  the list of hosts containing the block, possibly null
   */
  public FileSplitGroupRaster3D(Path[] files, long length, String[] hosts, int splitId, int cellXDim, int cellYDim,
                                int cellZDim, int groupXSize, int groupYSize, int groupZSize, boolean isFirstZGroup,
                                int radius) {
    this.files = files;
    this.length = length;
    this.hosts = hosts;
    this.splitId = splitId;

    this.cellXDim = cellXDim;
    this.cellYDim = cellYDim;
    this.cellZDim = cellZDim;
    this.groupXSize = groupXSize;
    this.groupYSize = groupYSize;
    this.groupZSize = groupZSize;
    this.radius = radius;
    this.isFristZGroup = isFirstZGroup;
//    this.isFirstColGroup = isFirstColGroup;
  }

  /**
   * The file containing this split's data.
   */
  public Path[] getPaths() {
    return files;
  }

  /**
   * The number of bytes in the file to process. */
  @Override
  public long getLength() {
    return length;
  }

  @Override
  public String[] getLocations() {
    if (this.hosts == null) {
      return new String[]{};
    } else {
      return this.hosts;
    }
  }


  /**
   * Serialize the fields of this object to <code>out</code>.
   *
   * @param out <code>DataOuput</code> to serialize this object into.
   * @throws IOException may happen in writeInt and writeLong
   */
  @Override
  public void write(DataOutput out) throws IOException {
    // files
    out.writeInt(files.length);
    for (Path p : files) {
      Text.writeString(out, p.toString());
    }

    out.writeLong(length); // length

    out.writeInt(hosts.length); // hosts
    for (String s : hosts) {
      Text.writeString(out, s);
    }

    out.writeInt(splitId); // splitId
    out.writeInt(cellXDim);
    out.writeInt(cellYDim);
    out.writeInt(cellZDim);
    out.writeInt(groupXSize);
    out.writeInt(groupYSize);
    out.writeInt(groupZSize);
    out.writeInt(radius);
    out.writeBoolean(isFristZGroup);
//    out.writeBoolean(isFirstColGroup);
  }

  /**
   * Deserialize the fields of this object from <code>in</code>.
   * <p>
   * <p>For efficiency, implementations should attempt to re-use storage in the
   * existing object where possible.</p>
   *
   * @param in <code>DataInput</code> to deseriablize this object from.
   * @throws IOException may happen in inputstream
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    // files
    files = new Path[in.readInt()];
    for (int i = 0; i < files.length; i++) {
      files[i] = new Path(Text.readString(in));
    }

    length = in.readLong();

    hosts = new String[in.readInt()];
    for (int i = 0; i < hosts.length; i++) {
      hosts[i] = Text.readString(in);
    }

    splitId = in.readInt();
    cellXDim = in.readInt();
    cellYDim = in.readInt();
    cellZDim = in.readInt();
    groupXSize = in.readInt();
    groupYSize = in.readInt();
    groupZSize = in.readInt();
    radius = in.readInt();
    isFristZGroup = in.readBoolean();
  }

//  this method is useless if data in hdfs all storing in disk
  // @Override
//  @InterfaceStability.Evolving
//  public SplitLocationInfo[] getLocationInfo() throws IOException {
//    return hostInfos;
//  }
}
