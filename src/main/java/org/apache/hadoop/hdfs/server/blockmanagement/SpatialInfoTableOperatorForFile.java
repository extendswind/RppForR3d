package org.apache.hadoop.hdfs.server.blockmanagement;

import com.cug.rpp4raster2d.util.Coord;
import com.cug.rpp4raster2d.util.GroupCellInfo;

import java.io.*;

/**
 * storage the spatial information table through file
 * every 3d raster file will using a file in the path
 */
public class SpatialInfoTableOperatorForFile extends SpatialInfoTableOperator{

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
   * @param info               grid cell info
   * @param filename           file path
   */
  public void saveGroupPosition( String filename, DatanodeDescriptor datanodeDescriptor, GroupCellInfo info,
                                        String groupType) {
    //    String nodePos = datanodeDescriptor.getNetworkLocation() + "/" + datanodeDescriptor.getName();
    //    nodeGroupCount.put(nodePos, nodeGroupCount.get(nodePos) + 1);

    FileOutputStream fileOutputStream;
    try {
      fileOutputStream = new FileOutputStream(new File(groupInfoFilePrefix + filename), true);
      BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
      bufferedWriter.write(groupType + " ");
      bufferedWriter.write(info.rowId + " ");
      bufferedWriter.write(info.colId + " ");
      bufferedWriter.write(datanodeDescriptor.getNetworkLocation() + "/" +
          datanodeDescriptor.getName());
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
  @Override
  public String readGroupPosition(String filename, Coord groupCoord, String type) {

    try (BufferedReader br = new BufferedReader(new FileReader(groupInfoFilePrefix + filename))) {
      String line = br.readLine();

      while (line != null) {
        String[] lineSplit = line.split(" ");
        String lineType = lineSplit[0];
        int lineRowId = Integer.parseInt(lineSplit[1]);
        int lineColId = Integer.parseInt(lineSplit[2]);
        if (type.equals(lineType) && groupCoord.rowId == lineRowId && groupCoord.colId == lineColId) {
          return lineSplit[3];
        }
        line = br.readLine();
      }
    } catch (IOException e) {
      //      e.printStackTrace();
      return null;
    }
    return null;
  }



}
