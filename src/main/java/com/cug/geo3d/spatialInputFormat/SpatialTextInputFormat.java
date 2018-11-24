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

import com.google.common.base.Charsets;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefaultSpatial;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StopWatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and values are the line of text..
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SpatialTextInputFormat extends FileInputFormat<LongWritable, Text> {

  private static final Log LOG = LogFactory.getLog(SpatialTextInputFormat.class);
  private long rowCellSize = 0;
  private long colCellSize = 0;
  private String spatialFilepath;
  private String spatialFilename;

//  public void setGridSize(long rowCellSize, long colCellSize, String spatialFilepath){
//    this.rowCellSize = rowCellSize;
//    this.colCellSize = colCellSize;
//    this.spatialFilepath = spatialFilepath;
//
//  }

  @Override
  public RecordReader<LongWritable, Text>
  createRecordReader(InputSplit split,
                     TaskAttemptContext context) {
    String delimiter = context.getConfiguration().get(
            "textinputformat.record.delimiter");
    byte[] recordDelimiterBytes = null;
    if (null != delimiter)
      recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
    return new SpatialRecordReader(recordDelimiterBytes);
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
    long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
    long maxSize = getMaxSplitSize(job);

    // generate splits
    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<FileStatus> files = listStatus(job);

    String dir = job.getConfiguration().get(INPUT_DIR, "");
    if ("" == dir) {
      throw new IOException("no input directory");
    }

    String infoFilename = FilenameUtils.getName(dir);
    String[] test = infoFilename.split("_");
    System.out.println(Integer.getInteger(test[0]));

    rowCellSize = Integer.parseInt(infoFilename.split("_")[1]);
    colCellSize = Integer.parseInt(infoFilename.split("_")[2]);

    // dir  ..../filename/info_rowSize_colSize

    // ..../filename/
    spatialFilepath = FilenameUtils.getPath(dir);

    // filename
    spatialFilename = FilenameUtils.getName(spatialFilepath.substring(0, spatialFilepath.length() - 1));


    FileSystem fs = new Path(dir).getFileSystem(job.getConfiguration());

    for (int i = 0; i < rowCellSize - 1; i++) {
      for (int j = 0; j < colCellSize - 1; j++) {

        HashMap<String, Integer> nodeCount = new HashMap<>();

        for (int ii = 0; ii < 2; ii++) {
          for (int jj = 0; jj < 2; jj++) {
            String cellName1 = spatialFilepath + BlockPlacementPolicyDefaultSpatial.GRID_INDEX_PREFIX + "_" +
                    spatialFilename + "_" + Integer.toString(i+ii)  + "_" + Integer.toString(j+jj);
            BlockLocation[] blkLocations = fs.getFileBlockLocations(new Path(cellName1), 0, 1);
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

        // TODO 检测nodeCount中value为4的key,然后makeSplit
        //for ()


      }
    }


    for (FileStatus file : files) {
      Path path = file.getPath();
      long length = file.getLen();
      if (length != 0) {
        BlockLocation[] blkLocations;
        if (file instanceof LocatedFileStatus) {
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          FileSystem fs = path.getFileSystem(job.getConfiguration());
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }
        if (isSplitable(job, path)) {
          long blockSize = file.getBlockSize();
          long splitSize = computeSplitSize(blockSize, minSize, maxSize);

          long bytesRemaining = length;
          while (((double) bytesRemaining) / splitSize > 1.1) {
            int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
            splits.add(makeSplit(path, length - bytesRemaining, splitSize,
                    blkLocations[blkIndex].getHosts(),
                    blkLocations[blkIndex].getCachedHosts()));
            bytesRemaining -= splitSize;
          }

          if (bytesRemaining != 0) {
            int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
            splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining,
                    blkLocations[blkIndex].getHosts(),
                    blkLocations[blkIndex].getCachedHosts()));
          }
        } else { // not splitable
          splits.add(makeSplit(path, 0, length, blkLocations[0].getHosts(),
                  blkLocations[0].getCachedHosts()));
        }
      } else {
        //Create empty hosts array for zero length files
        splits.add(makeSplit(path, 0, length, new String[0]));
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

  InputSplit makeEdgeSplit(Path[] files, long length, String[] hosts) {
    return new EdgeFileSplit(files, length, hosts);
  }

}
