package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

/**
 * 用于实现一个简单的分组策略的帮助，副本放置信息已经事先放在了存储的位置。
 * <p>
 * 仅为了对比实验，不实用
 */
public class SimpleGroupPlacementHelpler {

  public static final String RASTER_3D_SIMPLE_GROUP_PREFIX = "r3dsg";  // 使用raster 3d索引的文件，文件名会以此变量开头
  public static String placementFilePath =
      "/home/sparkl/hadoop_data/master/group_info_table_dir/r3dsg_placement.dat";  // 默认的路径

  public static boolean isRaster3DSimpleGroupFile(String filename) {
    String[] strs = filename.split("_");
    if(!strs[0].equals(RASTER_3D_SIMPLE_GROUP_PREFIX) || strs.length != 5){
      return false;
    } else {
      return true;
    }
  }

  public static void setDefaultPlacementFilePath(String path) {
    placementFilePath = path;
  }

  /**
   * 直接读文件返回一个存所有文件位置的Map
   *
   * TODO : 单元测试已经写好
   */
  public static HashMap<String, String[]> getPlacements() {
    Scanner scanner = null;
    try {
      scanner = new Scanner(new File(placementFilePath));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    HashMap<String, String[]> result = new HashMap<>();
    String line;
    while(scanner.hasNextLine()){
      line = scanner.nextLine();
      String[] strs = line.split(" ");
      if(strs.length != 4){
        continue;
      }
      result.put(strs[0], new String[]{strs[1], strs[2], strs[3]});
    }
    return result;
  }

}
