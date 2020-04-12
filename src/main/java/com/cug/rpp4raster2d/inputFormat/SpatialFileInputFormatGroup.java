package com.cug.rpp4raster2d.inputFormat;

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

import com.cug.rpp4raster3d.util.GroupInfo;
import com.cug.rpp4raster3d.util.SpatialConstant;
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
 * TODO  change name, remove text
 * An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and data are the line of text..
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SpatialFileInputFormatGroup extends FileInputFormat<LongWritable, InputSplitWritable> {

  private static final Log LOG = LogFactory.getLog(SpatialFileInputFormatGroup.class);
  private int cellRowNum;
  private int cellColNum;
  private int cellRowSize;
  private int cellColSize;
  private String spatialFilepath;
  private String spatialFilename;
  public GroupInfo groupInfo = GroupInfo.getDefaultGroupInfo(); // TODO
  private int radius;

  //  public void setGridSize(long cellRowNum, long cellColNum, String spatialFilepath){
  //    this.cellRowNum = cellRowNum;
  //    this.cellColNum = cellColNum;
  //    this.spatialFilepath = spatialFilepath;
  //
  //  }

  @Override
  public RecordReader<LongWritable, InputSplitWritable> createRecordReader(InputSplit split,
                                                                           TaskAttemptContext context) {
    return new SpatialRecordReaderGroup();
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
    radius = job.getConfiguration().getInt("rpp4raster3d.spatial.radius", 10);

    String infoFilename = FilenameUtils.getName(dir);

    cellRowNum = Integer.parseInt(infoFilename.split("_")[1]);
    cellColNum = Integer.parseInt(infoFilename.split("_")[2]);
    cellRowSize = Integer.parseInt(infoFilename.split("_")[3]);
    cellColSize = Integer.parseInt(infoFilename.split("_")[4]);

    int groupRowNum = (int) Math.ceil((double) cellRowNum / groupInfo.rowSize);
    int groupColNum = cellColNum <= groupInfo.colSize ? 1 :
        1 + (int) Math.ceil((double) (cellColNum - groupInfo.colSize) / (groupInfo.colSize - groupInfo.colOverlapSize));

    int fileNum = cellRowNum * cellColNum;  // number of all files

    // ..../filename/
    spatialFilepath = FilenameUtils.getPath(dir);

    // filename
    spatialFilename = FilenameUtils.getName(spatialFilepath.substring(0, spatialFilepath.length() - 1));

    FileSystem fs = new Path(dir).getFileSystem(job.getConfiguration());

    // 前一列的groupFirstRowCell + groupInfo.colSize < cellColNum 然后化简 ， 行同理
    //    for (int groupFirstRowId = 0; groupFirstRowId < cellRowNum; groupFirstRowId += groupInfo.rowSize) {
    //      for (int groupFirstColId = 0; groupFirstColId + groupInfo.colOverlapSize < cellColNum;
    //           groupFirstColId += groupInfo.colSize - groupInfo.colOverlapSize) {
    //

    for (int groupRowId = 0; groupRowId < groupRowNum; groupRowId++) {
      for (int groupColId = -1; groupColId < groupColNum; groupColId++) {

        int groupFirstColId;
        int groupFirstRowId;
        int groupRowSize;
        int groupColSize;
        int splitId;
        boolean isFirstGroup;

        if (groupColId == -1) { // for overlapped row group
          if (groupRowId == groupRowNum - 1) {
            continue;
          }

          int usedCellRowNum = (int) Math.ceil((double) radius / cellRowSize); // how many cells the radius will cover
          if (usedCellRowNum > groupInfo.rowOverlapSize)  // 半径太大而文件太少
          {
            LOG.error("the radius is too large for overlapped row group");
            return null;
          }

          groupFirstRowId = groupInfo.rowSize * (groupRowId + 1) - usedCellRowNum;
          groupRowSize = usedCellRowNum * 2;
          groupFirstColId = 0;
          groupColSize = cellColNum;
          splitId = groupRowId + SpatialConstant.ROW_OVERLAPPED_GROUP_SPLIT_ID_BEGIN;
          isFirstGroup = true;
        } else { // for normal group
          groupFirstColId = groupColId * (groupInfo.colSize - groupInfo.colOverlapSize);
          groupFirstRowId = groupRowId * groupInfo.rowSize;

          // maybe the row size of most bottom group is less than the groupInfo.rowSize
          groupRowSize = groupFirstRowId + groupInfo.rowSize > cellRowNum ? cellRowNum - groupFirstRowId :
              groupInfo.rowSize;
          groupColSize = (groupFirstColId + groupInfo.colSize > cellColNum) ? cellColNum - groupFirstColId :
              groupInfo.colSize;
          splitId = groupColId + groupRowId * groupRowNum;
          isFirstGroup = (groupColId == 0);
        }


        Path[] groupFilePaths = new Path[groupRowSize * groupColSize];

        // get the file path and information of file block locations
        for (int i = 0; i < groupRowSize; i++) {
          for (int j = 0; j < groupColSize; j++) {
            String cellName = spatialFilepath + SpatialConstant.GRID_INDEX_PREFIX + "_" + spatialFilename + "_" +
                (groupFirstRowId + i) + "_" + (groupFirstColId + j);
            groupFilePaths[j + i * groupColSize] = new Path(cellName);
            //            FileStatus fileStatus = new FileStatus();//fs.getFileStatus(new Path(cellName));
         }
        }

        String[] hosts = getHostsFromPaths(groupFilePaths, fs);

        splits.add(new FileSplitGroup(groupFilePaths, 1, hosts, splitId, cellRowSize,
            cellColSize, groupRowSize, groupColSize, isFirstGroup, radius));
      }
    }

    // Save the number of input files for metrics/loadgen
    job.getConfiguration().setLong(NUM_INPUT_FILES, fileNum);


    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Total # of splits generated by getSplits: " + splits.size() + ", TimeTaken: " + sw
          .now(TimeUnit.MILLISECONDS));
    }
    return splits;
  }

  /**
   * 哪个host中包含的文件多就选哪个host
   *
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

  //  InputSplit makeEdgeSplit(Path[] files, long length, String[] hosts) {
  //    return new EdgeFileSplit(files, length, hosts);
  //  }

}
