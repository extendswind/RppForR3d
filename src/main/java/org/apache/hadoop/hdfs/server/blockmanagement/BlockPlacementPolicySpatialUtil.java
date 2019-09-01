package org.apache.hadoop.hdfs.server.blockmanagement;

import com.cug.rpp4raster2d.util.CellIndexInfo;

import java.io.*;

@Deprecated
public class BlockPlacementPolicySpatialUtil {

  /**
   * save current datanode information of chosen result
   *
   * @param results  current chosen result
   * @param info     grid cell info
   * @param filepath file path
   */
  public static void saveGridIndexToFile(DatanodeStorageInfo[] results, CellIndexInfo info, String filepath) {
    if (info.rowId == 0 && info.colId == 0)
      new File(filepath).delete();
    if (results.length == 0)
      return;
    FileOutputStream fileOutputStream;
    try {
      fileOutputStream = new FileOutputStream(new File(filepath), true);
      BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
      bufferedWriter.write(info.rowId + " ");
      bufferedWriter.write(info.colId + " ");
      bufferedWriter.write(results[0].getDatanodeDescriptor().getNetworkLocation() + "/" +
          results[0].getDatanodeDescriptor().getName());
      for (int i = 1; i < results.length; i++) {
        bufferedWriter.write(" ");
        bufferedWriter.write(results[i].getDatanodeDescriptor().getNetworkLocation() + "/" +
            results[i].getDatanodeDescriptor().getName());
      }
      bufferedWriter.write("\n");

      // for data balance
      if (info.rowId == 0 && info.colId != 0) {
        bufferedWriter.write("-1 -1 ");
        bufferedWriter.write(results[0].getDatanodeDescriptor().getNetworkLocation() + "/" +
            results[0].getDatanodeDescriptor().getName());
        bufferedWriter.write("\n");
      }


      bufferedWriter.flush();
      bufferedWriter.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  /**
   * input file and info
   * get corresponding datanodeLocations of infos from file
   */
  public static String[][] readGridIndexFromFile(String file, CellIndexInfo[] infos) {

    String[][] datanodeLocs = new String[infos.length][];
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line = br.readLine();

      while (line != null) {
        String[] lineSplit = line.split(" ");
        Long lineRowId = Long.parseLong(lineSplit[0]);
        Long lineColId = Long.parseLong(lineSplit[1]);

        for (int i = 0; i < infos.length; i++) {
          if (infos[i].rowId == lineRowId && infos[i].colId == lineColId) {
            datanodeLocs[i] = new String[lineSplit.length - 2];
            System.arraycopy(lineSplit, 2, datanodeLocs[i], 0, lineSplit.length - 2);
          }
        }
        line = br.readLine();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return datanodeLocs;
  }





}
