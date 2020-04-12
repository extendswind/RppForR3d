package org.apache.hadoop.hdfs.server.blockmanagement;

import com.cug.rpp4raster3d.util.Coord;

import java.io.*;

/**
 * storage the spatial information table through file
 * every 3d raster file will using a file in the path
 */
public class SpatialInfoTableOperatorForFile extends SpatialInfoTableOperator{
  private final String DIMS_TYPE = "DIMS";

  private String groupInfoFilePrefix;

  // 假设上传文件的位置
  public SpatialInfoTableOperatorForFile(){
    this("/tmp");
  }

  public SpatialInfoTableOperatorForFile(String spatialInfoTableFilePath){
    groupInfoFilePrefix = spatialInfoTableFilePath + "/GROUP_" ;
  }

  @Override
  public void clearTable(String filename) {
    new File(groupInfoFilePrefix + filename).delete();
  }

  /**
   * save datanode and group information to the table
   *
   * @param datanodeDescriptor current chosen result
   * @param coord               grid cell info
   * @param filename           file path
   */
  @Override
  public void saveGroupPosition( String filename, DatanodeDescriptor datanodeDescriptor, Coord coord,
                                        String groupType) {
    String datanodeStr = datanodeDescriptor.getNetworkLocation() + "/" + datanodeDescriptor.getName();
    saveGroupString(filename, datanodeStr, coord, groupType);
  }

  private void saveGroupString(String filename, String groupString, Coord coord, String groupType) {
    FileOutputStream fileOutputStream;
    try {
      fileOutputStream = new FileOutputStream(new File(groupInfoFilePrefix + filename), true);
      BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
      bufferedWriter.write(groupType + " ");
      bufferedWriter.write(coord.xId + " ");
      bufferedWriter.write(coord.yId + " ");
      bufferedWriter.write(coord.zId + " ");
      bufferedWriter.write(groupString);
      bufferedWriter.newLine();
      bufferedWriter.flush();
      bufferedWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * input file and info
   * get corresponding datanodeLocations of groupCoord from file
   */
  private String readGroupString(String filename, Coord groupCoord, String type) {

    try (BufferedReader br = new BufferedReader(new FileReader(groupInfoFilePrefix + filename))) {
      String line = br.readLine();

      while (line != null) {
        String[] lineSplit = line.split(" ");
        String lineType = lineSplit[0];
        int lineColId = Integer.parseInt(lineSplit[1]);
        int lineRowId = Integer.parseInt(lineSplit[2]);
        int lineZId = Integer.parseInt(lineSplit[3]);
        if (type.equals(lineType) && groupCoord.yId == lineRowId && groupCoord.xId == lineColId && groupCoord.zId == lineZId) {
          return lineSplit[4];
        }
        line = br.readLine();
      }
    } catch (IOException e) {
      //      e.printStackTrace();
      return null;
    }
    return null;
  }

  /**
   * input file and info
   * get corresponding datanodeLocations of groupCoord from file
   */
  @Override
  public String readGroupPosition(String filename, Coord groupCoord, String type) {
    return readGroupString(filename, groupCoord, type);
  }

  @Override
  public void saveR3dDimensions(String filename, int xDim, int yDim, int zDim){
    String dims = xDim + "-" + yDim + "-" + zDim;
    saveGroupString(filename, dims, new Coord(-1, -1, -1), DIMS_TYPE);
  }

  @Override
  public int[] readR3dDimensions(String filename){
    String dimsStr = readGroupString(filename, new Coord(-1, -1, -1), DIMS_TYPE);
    if(dimsStr == null)
      return null;
    String[] dimStrs = dimsStr.split("-");
    int[] dims = new int[3];
    for(int i=0; i<3; i++){
      dims[i] = Integer.parseInt(dimStrs[i]);
    }
    return dims;
  }

}
