package com.cug.rpp4raster3d.spatialInputFormat;

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

import com.cug.rpp4raster3d.util.SpatialConstant;
import com.cug.rpp4raster3d.raster3d.Raster3D;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StopWatch;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * An {@link InputFormat} for 3d raster data.
 * 一个简单的实现，以每个文件为单位进行单独分析
 * 读取整个文件以及周边文件半径R内的部分，
 * every file will be used as an InputSplit
 * every file use data of surround files in a certain radius
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SpatialFileInputFormatSimpleRaster3D extends FileInputFormat<LongWritable, Raster3D> {
  private static final Log LOG = LogFactory.getLog(SpatialFileInputFormatSimpleRaster3D.class);

  @Override
  public RecordReader<LongWritable, Raster3D> createRecordReader(InputSplit split,
                                                                 TaskAttemptContext context) {
    return new SpatialRecordReaderSimpleRaster3D();
  }


  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }


  /**
   * input: spatial file path(through job)
   * <p>
   * Generate the list of files and make them into FileSplits.
   *
   * @param job the job context
   * @throws IOException when INPUT_DIR is not set
   */
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    StopWatch sw = new StopWatch().start();
    List<InputSplit> splits = new ArrayList<>();

    String dir = job.getConfiguration().get(INPUT_DIR, "");
    if ("".equals(dir)) {
      throw new IOException("no input directory");
    }
    int radius = job.getConfiguration().getInt("rpp4raster3d.spatial.radius", 10);
    LOG.info("spatial radius is set to " + radius);

    String infoFilename = FilenameUtils.getName(dir);
    String[] filenameSplits = infoFilename.split("_");

    // number of cells in x direction
    int cellXNum = Integer.parseInt(filenameSplits[2]);
    int cellYNum = Integer.parseInt(filenameSplits[3]);
    int cellZNum = Integer.parseInt(filenameSplits[4]);
    // dimension of a cell in x direction
    int cellXDim = Integer.parseInt(filenameSplits[5]);
    int cellYDim = Integer.parseInt(filenameSplits[6]);
    int cellZDim = Integer.parseInt(filenameSplits[7]);
        // ..../filename/
    String spatialFilepath = FilenameUtils.getPath(dir);

    // filename
    String spatialFilename = FilenameUtils.getName(spatialFilepath.substring(0, spatialFilepath.length() - 1));


    FileSystem fs = new Path(dir).getFileSystem(job.getConfiguration());

    for (int z = 0; z < cellZNum; z++) {
      for (int y = 0; y < cellYNum; y++) {
        for (int x = 0; x < cellXNum; x++) {
          int leftX = Math.max(x - 1, 0);
          int leftY = Math.max(y - 1, 0);
          int leftZ = Math.max(z - 1, 0);
          int rightX = Math.min(x + 2, cellXNum);
          int rightY = Math.min(y + 2, cellYNum);
          int rightZ = Math.min(z + 2, cellZNum);

          Path[] groupFilePaths = new Path[(rightX - leftX) * (rightY - leftY) * (rightZ - leftZ)];
          for (int zz = leftZ; zz < rightZ; zz++) {
            for (int yy = leftY; yy < rightY; yy++) {
              for (int xx = leftX; xx < rightX; xx++) {
                String cellName = spatialFilepath + "/" + filenameSplits[0] + "_" +
                    spatialFilename + "_" + (xx) + "_" + (yy) + "_" + (zz);
                groupFilePaths[xx - leftX + (yy - leftY) * (rightX - leftX) + (zz - leftZ) * (rightX - leftX) * (rightY - leftY)] =
                    new Path(cellName);
              }
            }
          }

          String currentCell = spatialFilepath + "/" + filenameSplits[0] + "_" +
              spatialFilename + "_" + (x) + "_" + (y) + "_" + (z);
          BlockLocation[] blkLocations = fs.getFileBlockLocations(new Path(currentCell), 0, 1);
          String[] blkHosts = blkLocations[0].getHosts();
          blkHosts = sortBlockHosts(blkHosts, groupFilePaths, fs);

          int splitId = x + y * cellXNum + z * cellXNum * cellYNum;

          // isFirstZGroup is useless
          splits.add(new FileSplitGroupRaster3D(groupFilePaths, 1, blkHosts, splitId, cellXDim, cellYDim, cellZDim,
              rightX - leftX, rightY - leftY, rightZ - leftZ, false, radius));
        }
      }
    }

    // Save the number of input files for metrics/loadgen
    job.getConfiguration().setLong(NUM_INPUT_FILES, cellXNum * cellYNum * cellZNum);

    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Total # of splits generated by getSplits: " + splits.size() + ", TimeTaken: " + sw
          .now(TimeUnit.MILLISECONDS));
    }
    return splits;
  }

  /**
   *  Sort block host
   *  对主文件的三个副本所在数据节点，根据节点上的其它文件数量进行排序
   */
  private String[] sortBlockHosts(String[] blockHosts, Path[] paths, FileSystem fs) throws IOException {
    // count file block locations
    TreeMap<String, Integer> nodeCount = new TreeMap<>();
    for(String str: blockHosts){
      nodeCount.put(str, 1);
    }
    for (Path path : paths) {
      BlockLocation[] blkLocations = fs.getFileBlockLocations(path, 0, 1);
      String[] blkHosts = blkLocations[0].getHosts();
      for (String host : blkHosts) {
        if (nodeCount.containsKey(host)) {
          nodeCount.put(host, nodeCount.get(host) + 1);
        }
      }
    }
    // 对nodeCount根据value排序后返回key的数组
    return nodeCount.entrySet().stream()
        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
        .map(a->a.getKey()).toArray(size -> new String[size]);
  }
}
