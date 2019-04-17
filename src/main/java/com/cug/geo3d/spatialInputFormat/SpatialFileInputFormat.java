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

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefaultSpatial;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StopWatch;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * TODO  change name, remove text
 * An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and data are the line of text..
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SpatialFileInputFormat extends FileInputFormat<LongWritable, InputSplitWritable> {

  private static final Log LOG = LogFactory.getLog(SpatialFileInputFormat.class);
  private int gridRowSize = 0;
  private int gridColSize = 0;
  private int cellRowSize = 0;
  private int cellColSize = 0;
  private String spatialFilepath;
  private String spatialFilename;

//  public void setGridSize(long gridRowSize, long gridColSize, String spatialFilepath){
//    this.gridRowSize = gridRowSize;
//    this.gridColSize = gridColSize;
//    this.spatialFilepath = spatialFilepath;
//
//  }

  @Override
  public RecordReader<LongWritable, InputSplitWritable>
  createRecordReader(InputSplit split,
                     TaskAttemptContext context) {

    try {
      return new SpatialRecordReader();
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
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
   * @throws IOException
   */
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    StopWatch sw = new StopWatch().start();
//    long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
//    long maxSize = getMaxSplitSize(job);

    // generate splits
    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<FileStatus> files = listStatus(job);

    String dir = job.getConfiguration().get(INPUT_DIR, "");
    if ("" == dir) {
      throw new IOException("no input directory");
    }

    String infoFilename = FilenameUtils.getName(dir);

    gridRowSize = Integer.parseInt(infoFilename.split("_")[1]);
    gridColSize = Integer.parseInt(infoFilename.split("_")[2]);
    cellRowSize = Integer.parseInt(infoFilename.split("_")[3]);
    cellColSize = Integer.parseInt(infoFilename.split("_")[4]);


    // dir  ..../filename/info_rowSize_colSize

    // ..../filename/
    spatialFilepath = FilenameUtils.getPath(dir);

    // filename
    spatialFilename = FilenameUtils.getName(spatialFilepath.substring(0, spatialFilepath.length() - 1));

    FileSystem fs = new Path(dir).getFileSystem(job.getConfiguration());

    for (int i = 0; i < gridRowSize - 1; i++) {
      for (int j = 0; j < gridColSize - 1; j++) {

        // to get a best host
        HashMap<String, Integer> nodeCount = new HashMap<>();
        Path[] edgeFilePaths = new Path[4];

        // count blocks of four edge file
        for (int ii = 0; ii < 2; ii++) {
          for (int jj = 0; jj < 2; jj++) {
            String cellName = spatialFilepath + BlockPlacementPolicyDefaultSpatial.GRID_INDEX_PREFIX + "_" +
                    spatialFilename + "_" + Integer.toString(i + ii) + "_" + Integer.toString(j + jj);
            edgeFilePaths[jj + ii * 2] = new Path(cellName);
//            FileStatus fileStatus = new FileStatus();//fs.getFileStatus(new Path(cellName));
            BlockLocation[] blkLocations = fs.getFileBlockLocations(edgeFilePaths[jj + ii * 2], 0, 1);
            String[] blkHosts = blkLocations[0].getHosts();
            for (String host : blkHosts) {
              if (nodeCount.containsKey(host)) {
                nodeCount.put(host, nodeCount.get(host) + 1);
              } else {
                nodeCount.put(host, 1);
              }
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

        // if a host have all replicas of four cell
        List<String> hosts = new LinkedList<>();
        for (int ii = 0; ii < tempList.size(); ii++) {
          if (tempList.get(ii).getValue() == 4)
            hosts.add(tempList.get(ii).getKey());
        }

        // if no host have all replicas of four cell
        // put the first 3 which has most replicas
        if (hosts.isEmpty()) {
          for (int ii = 0; ii < 3 && ii < tempList.size(); ii++) {
            hosts.add(tempList.get(ii).getKey());
          }
        }

        int splitId = j + i * gridColSize;
        splits.add(new EdgeFileSplit(edgeFilePaths, 1, hosts.toArray(new String[hosts.size()]),
                splitId, gridRowSize, gridColSize, cellRowSize, cellColSize));

      }
    }

    // Save the number of input files for metrics/loadgen
    job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());


    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Total # of splits generated by getSplits: " + splits.size()
              + ", TimeTaken: " + sw.now(TimeUnit.MILLISECONDS));
    }
    return splits;
  }

//  InputSplit makeEdgeSplit(Path[] files, long length, String[] hosts) {
//    return new EdgeFileSplit(files, length, hosts);
//  }

}
