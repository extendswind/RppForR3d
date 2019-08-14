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

import com.cug.rpp4raster2d.util.GroupInfo;
import com.cug.rpp4raster2d.util.SpatialConstant;
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
 * An {@link InputFormat} for 3D raster data, designed specially for 3D raster ReplicaPlacementPolicy optimization
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SpatialFileInputFormatGroupRaster3D extends FileInputFormat<LongWritable, Raster3D> {
  private static final Log LOG = LogFactory.getLog(SpatialFileInputFormatGroupRaster3D.class);
  private int cellXNum;  // number of cells in x direction
  private int cellYNum;
  private int cellZNum;
  private int cellXDim; // dimension of a cell in x direction
  private int cellYDim;
  private int cellZDim;

  private String spatialFilepath;
  private String spatialFilename;
  private GroupInfo groupInfo = GroupInfo.getDefaultGroupInfo();
  private int radius;

  //  public void setGridSize(long cellRowNum, long cellColNum, String spatialFilepath){
  //    this.cellRowNum = cellRowNum;
  //    this.cellColNum = cellColNum;
  //    this.spatialFilepath = spatialFilepath;
  //
  //  }

  @Override
  public RecordReader<LongWritable, Raster3D> createRecordReader(InputSplit split,
                                                                       TaskAttemptContext context) {
    return new SpatialRecordReaderGroupRaster3D();
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

    LOG.info("using groupInfo:  " + groupInfo.rowSize + " " + groupInfo.colSize + " " + groupInfo.zSize);

    StopWatch sw = new StopWatch().start();

    // generate splits
    List<InputSplit> splits = new ArrayList<>();
    // List<FileStatus> files = listStatus(job);

    String dir = job.getConfiguration().get(INPUT_DIR, "");
    if ("".equals(dir)) {
      throw new IOException("no input directory");
    }
    radius = job.getConfiguration().getInt("rpp4raster3d.spatial.radius", 10);
    LOG.info("spatial radius is set to " + radius);

    String infoFilename = FilenameUtils.getName(dir);

    cellXNum = Integer.parseInt(infoFilename.split("_")[2]);
    cellYNum = Integer.parseInt(infoFilename.split("_")[3]);
    cellZNum = Integer.parseInt(infoFilename.split("_")[4]);
    cellXDim = Integer.parseInt(infoFilename.split("_")[5]);
    cellYDim = Integer.parseInt(infoFilename.split("_")[6]);
    cellZDim = Integer.parseInt(infoFilename.split("_")[7]);

    // number of groups in x dimension
    int groupColNum = cellXNum <= groupInfo.colSize ? 1 :
        1 + (int) Math.ceil((double) (cellXNum - groupInfo.colSize) / (groupInfo.colSize - groupInfo.colOverlapSize));
    int groupRowNum = (int) Math.ceil((double) cellYNum / groupInfo.rowSize);
    int groupZNum = (int) Math.ceil((double) cellZNum / groupInfo.zSize);
    int fileNum = cellXNum * cellYNum * cellZNum;  // number of all files

    // ..../filename/
    spatialFilepath = FilenameUtils.getPath(dir);

    // filename
    spatialFilename = FilenameUtils.getName(spatialFilepath.substring(0, spatialFilepath.length() - 1));

    FileSystem fs = new Path(dir).getFileSystem(job.getConfiguration());

    // 前一列的groupFirstRowCell + groupInfo.colSize < cellColNum 然后化简 ， 行同理
    //    for (int groupFirstRowId = 0; groupFirstRowId < cellRowNum; groupFirstRowId += groupInfo.rowSize) {
    //      for (int groupFirstColId = 0; groupFirstColId + groupInfo.colOverlapSize < cellColNum;
    //           groupFirstColId += groupInfo.colSize - groupInfo.colOverlapSize) {


    for (int groupZ = 0; groupZ < groupZNum; groupZ++) {
      for (int groupRowId = 0; groupRowId < groupRowNum; groupRowId++) {
        for (int groupColId = -1; groupColId < groupColNum; groupColId++) {

          int groupFirstColId;
          int groupFirstRowId;
          int groupFirstZId;
          int groupRowSize;
          int groupColSize;
          int groupZSize;
          int splitId;

          if(groupZ == 0 ){
            groupFirstZId = 0;
            groupZSize = groupInfo.zSize + 1;
            if(groupZSize > cellZNum)
              groupZSize = cellZNum;
          } else {
            groupFirstZId = groupInfo.zSize * groupZ - 1;
            if (groupZ == groupZNum - 1)
              groupZSize = groupInfo.zSize + 1;
            else
              groupZSize = groupInfo.zSize + 2;
            if(groupFirstZId + groupZSize > cellZNum){
              groupZSize = cellZNum - groupFirstZId;
            }
          }

          if (groupColId == -1) { // for overlapped row group
            if (groupRowId == groupRowNum - 1) {
              continue;
            }
            int usedCellRowNum = (int) Math.ceil((double) radius / cellXDim); // how many cells the radius will cover
            if (usedCellRowNum > groupInfo.rowOverlapSize)  // 半径太大而文件太少
            {
              LOG.error("the radius is too large for overlapped row group");
              return null;
            }
            groupFirstRowId = groupInfo.rowSize * (groupRowId + 1) - usedCellRowNum;
            groupRowSize = usedCellRowNum * 2;
            groupFirstColId = 0;
            groupColSize = cellXNum;
            splitId = groupZ * (groupRowNum-1) + groupRowId + SpatialConstant.ROW_OVERLAPPED_GROUP_SPLIT_ID_BEGIN;
          } else { // for normal group
            groupFirstColId = groupColId * (groupInfo.colSize - groupInfo.colOverlapSize);
            groupFirstRowId = groupRowId * groupInfo.rowSize;

            // maybe the row size of most bottom group is less than the groupInfo.rowSize
            groupRowSize = groupFirstRowId + groupInfo.rowSize > cellYNum ? cellYNum - groupFirstRowId :
                groupInfo.rowSize;
            groupColSize = (groupFirstColId + groupInfo.colSize > cellXNum) ? cellXNum - groupFirstColId :
                groupInfo.colSize;
            splitId = groupColId + groupRowId * groupColNum + groupZ * groupRowNum * groupColNum;
          }

          Path[] groupFilePaths = new Path[groupRowSize * groupColSize * groupZSize];


          int mainFileZSize = groupInfo.zSize;
          if(groupInfo.zSize > groupZSize)
            mainFileZSize = groupZSize;  // 当groupInfo.zSize > groupZSize的情况
          if(groupFirstZId != 0 && groupFirstZId + groupInfo.zSize >= cellZNum){
            mainFileZSize = cellZNum - groupFirstZId - 1;
          }
          Path[] mainFilePaths = new Path[groupRowSize * groupColSize * mainFileZSize]; // used for get the right host

          for(int zz = 0; zz < groupZSize; zz++) {
            // get the file path and information of file block locations
            for (int yy = 0; yy < groupRowSize; yy++) {
              for (int xx = 0; xx < groupColSize; xx++) {
                String cellName = spatialFilepath  + "/" + SpatialConstant.RASTER_3D_INDEX_PREFIX + "_" +
                    spatialFilename + "_" + (groupFirstColId + xx) + "_" + (groupFirstRowId + yy) + "_" + (groupFirstZId + zz);
                int fileIndex = xx + yy * groupColSize + zz * groupColSize * groupRowSize;
                groupFilePaths[fileIndex] = new Path(cellName);


                if(groupFirstZId + zz >= groupZ * groupInfo.zSize && groupFirstZId + zz < (groupZ+1)* groupInfo.zSize)
                  mainFilePaths[xx + yy * groupColSize + (groupFirstZId +zz - groupZ* groupInfo.zSize) * groupRowSize * groupColSize] =
                      new Path(cellName);
              }
            }
          }
          if(mainFilePaths[0] == null)
            System.out.println("DEBUG error");

          String[] hosts = getHostsFromPaths(mainFilePaths, fs);

          boolean isFirstZGroup = (groupZ == 0);
          splits.add(new FileSplitGroupRaster3D(groupFilePaths, 1, hosts, splitId, cellXDim, cellYDim, cellZDim,
              groupColSize, groupRowSize, groupZSize, isFirstZGroup, radius));
        }
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
