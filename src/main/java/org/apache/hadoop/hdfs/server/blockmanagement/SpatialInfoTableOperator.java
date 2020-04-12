package org.apache.hadoop.hdfs.server.blockmanagement;


import com.cug.rpp4raster3d.util.CellIndexInfo;
import com.cug.rpp4raster3d.util.Coord;

public abstract class SpatialInfoTableOperator {

  /**
   * clear table information of the uploaded file
   */
  public abstract void clearTable(String filename);

  /**
   * save datanode and group information to the table
   *
   * @param datanodeDescriptor current chosen result
   * @param coord               grid cell position
   * @param filename           filename of uploaded 3d raster file
   */
  public abstract void saveGroupPosition(String filename, DatanodeDescriptor datanodeDescriptor, Coord coord,
                                         String groupType);

  /**
     * input 3d raster file name and info
     * get datanode of groupCoord from file
     */
  public abstract String readGroupPosition(String file, Coord groupCoord, String type) ;

  public abstract void saveR3dDimensions(String filename, int xDim, int yDim, int zDim);

  public abstract int[] readR3dDimensions(String filename);

}
