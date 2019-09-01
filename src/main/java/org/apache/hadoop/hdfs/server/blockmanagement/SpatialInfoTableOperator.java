package org.apache.hadoop.hdfs.server.blockmanagement;


import com.cug.rpp4raster2d.util.Coord;
import com.cug.rpp4raster2d.util.GroupCellInfo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public abstract class SpatialInfoTableOperator {

  /**
   * clear table information of the uploaded file
   */
  public abstract void clearTable(String filename);

  /**
   * save datanode and group information to the table
   *
   * @param datanodeDescriptor current chosen result
   * @param info               grid cell info
   * @param filename           filename of uploaded 3d raster file
   */
  public abstract void saveGroupPosition(String filename, DatanodeDescriptor datanodeDescriptor, GroupCellInfo info,
                                 String groupType);

    /**
     * input 3d raster file name and info
     * get datanode of groupCoord from file
     */
  public abstract String readGroupPosition(String file, Coord groupCoord, String type) ;
}
