package com.cug.geo3d.spatialInputFormat;

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

import com.cug.geo3d.util.GroupInfo;
import com.cug.geo3d.util.SpatialConstant;
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
 * An {@link InputFormat} for spatial raster data processing.
 *
 * key is the file number in row order
 * value is the file content with right and bottom area in the width of 2 * radius
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SpatialFileInputFormatOrdinary extends FileInputFormat<LongWritable, InputSplitWritable> {

  private static final Log LOG = LogFactory.getLog(SpatialFileInputFormatGroup.class);
  private int cellRowNum;
  private int cellColNum;
  private int cellRowSize;
  private int cellColSize;
  private String spatialFilePath;
  private String spatialFilename;
//  private GroupInfo groupInfo = GroupInfo.getDefaultGroupInfo();
  private int radius;


  @Override
  public RecordReader<LongWritable, InputSplitWritable> createRecordReader(InputSplit split,
                                                                           TaskAttemptContext context) {
    return new SpatialRecordReaderOrdinary();
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
    //    long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
    //    long maxSize = getMaxSplitSize(job);

    // generate splits
    List<InputSplit> splits = new ArrayList<>();
    // List<FileStatus> files = listStatus(job);

    String dir = job.getConfiguration().get(INPUT_DIR, "");
    if ("".equals(dir)) {
      throw new IOException("no input directory");
    }
    radius = job.getConfiguration().getInt("geo3d.spatial.radius", 10);

    String infoFilename = FilenameUtils.getName(dir);

    cellRowNum = Integer.parseInt(infoFilename.split("_")[1]);
    cellColNum = Integer.parseInt(infoFilename.split("_")[2]);
    cellRowSize = Integer.parseInt(infoFilename.split("_")[3]);
    cellColSize = Integer.parseInt(infoFilename.split("_")[4]);

    int fileNum = cellRowNum * cellColNum;  // number of all files

    // ..../filename/
    spatialFilePath = FilenameUtils.getPath(dir);

    // filename
    spatialFilename = FilenameUtils.getName(spatialFilePath.substring(0, spatialFilePath.length() - 1));

    FileSystem fs = new Path(dir).getFileSystem(job.getConfiguration());

    for (int i = 0; i < cellRowNum; i++) {
      for (int j = 0; j < cellColNum; j++) {
        Path[] groupFilePaths;
        if (i == cellRowNum - 1 && j != cellColNum - 1) { // bottom most
          groupFilePaths = new Path[2];
          groupFilePaths[0] = new Path(spatialFilePath + SpatialConstant.GRID_INDEX_PREFIX + "_" + spatialFilename +
              "_" + (i) + "_" + (j));
          groupFilePaths[1] = new Path(spatialFilePath + SpatialConstant.GRID_INDEX_PREFIX + "_" + spatialFilename +
              "_" + (i) + "_" + (j + 1));
        } else if (i != cellRowNum - 1 && j == cellColNum - 1) {  // right most
          groupFilePaths = new Path[2];
          groupFilePaths[0] = new Path(spatialFilePath + SpatialConstant.GRID_INDEX_PREFIX + "_" + spatialFilename +
              "_" + (i) + "_" + (j));
          groupFilePaths[1] = new Path(spatialFilePath + SpatialConstant.GRID_INDEX_PREFIX + "_" + spatialFilename +
              "_" + (i + 1) + "_" + (j));
        } else if (i == cellRowNum - 1 && j == cellColNum - 1) {  // right bottom most
          groupFilePaths = new Path[1];
          groupFilePaths[0] = new Path(spatialFilePath + SpatialConstant.GRID_INDEX_PREFIX + "_" + spatialFilename +
              "_" + (i) + "_" + (j));
        } else {
          groupFilePaths = new Path[4];
          for (int ii = 0; ii < 2; ii++) {
            for (int jj = 0; jj < 2; jj++) {
              groupFilePaths[ii * 2 + jj] = new Path( spatialFilePath + SpatialConstant.GRID_INDEX_PREFIX + "_"
                  + spatialFilename + "_" + (i + ii) + "_" + (j + jj));
            }
          }
        }

        String[] hosts = getHostsFromPaths(groupFilePaths, fs);
        int splitId = i*cellColNum +j;


        splits.add(new FileSplitGroup(groupFilePaths, 1, hosts, splitId, cellRowSize,
            cellColSize, 1, 1, false, radius));
      }
    }

    // Save the number of input files for metrics/loadgen
    job.getConfiguration().

        setLong(NUM_INPUT_FILES, fileNum);


    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Total # of splits generated by getSplits: " + splits.size() + ", TimeTaken: " + sw
          .now(TimeUnit.MILLISECONDS));
    }
    return splits;
  }

  /**
   * given the paths of computing files, get the block location in hdfs, and sort the host of the first path
   * according to the block number
   *
   * to find the most proper host for the paths
   *
   * @param paths
   * @param fs
   * @return
   * @throws IOException
   */
  private String[] getHostsFromPaths(Path[] paths, FileSystem fs) throws IOException {
    // count file block locations
    HashMap<String, Integer> nodeCount = new HashMap<>();
    for (Path path : paths) {
      BlockLocation[] blkLocations = fs.getFileBlockLocations(path, 0, 1);
      String[] blkHosts = blkLocations[0].getHosts();
      for (String host : blkHosts) {
        if (nodeCount.containsKey(host)) {
          nodeCount.put(host, nodeCount.get(host) + 1);
        } else {
          nodeCount.put(host, 1);
        }
      }
    }

    // sort by the replica number of the host
    List<Map.Entry<String, Integer>> tempList = new LinkedList<>(nodeCount.entrySet());
    Collections.sort(tempList, new Comparator<Map.Entry<String, Integer>>() {
      @Override
      public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
        return o1.getValue().compareTo(o2.getValue()) * (-1);
      }
    });

    // add the hosts which have all replicas of cell
    List<String> hosts = new LinkedList<>();
    for (Map.Entry<String, Integer> stringIntegerEntry : tempList) {
      if (stringIntegerEntry.getValue() == paths.length) {
        hosts.add(stringIntegerEntry.getKey());
      }
    }

    // if no host have all replicas of four cell
    // put the first 3 which has most replicas
    if (hosts.isEmpty()) {
      for (int ii = 0; ii < 3 && ii < tempList.size(); ii++) {
        hosts.add(tempList.get(ii).getKey());
      }
    }

    return hosts.toArray(new String[0]);
  }


}

