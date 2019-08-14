package com.cug.rpp4raster3d.raster3dGenerate;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DataDistributionVerify {

  static String[][] getAllNodes() throws IOException {

    System.setProperty("HADOOP_USER_NAME", "sparkl");

    String localFile = "test_data/raster3d-3.dat";
    String hdfsDir = "hdfs://kvmmaster:9000/user/sparkl/rppo/" + FilenameUtils.getName(localFile); // + "_optimize";

    int cellXNum = 7;
    int cellYNum = 6;
    int cellZNum = 4;

    // get block location
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create("hdfs://kvmmaster:9000/"), conf);



    String[][] result = new String [cellXNum*cellYNum*cellZNum][];
    for(int i=0; i<cellZNum; i++){
      for(int j=0; j<cellYNum; j++){
        for(int k=0; k<cellXNum; k++){
          Path p = new Path(hdfsDir + "/r3d_" + FilenameUtils.getName(localFile) + "_" +
              k + "_" + j + "_" + i);
          result[i*cellXNum*cellYNum + j*cellXNum +k] = ((fs.getFileBlockLocations(p, 0, 1))[0].getHosts());
        }
      }
    }

    // count the block number in each datanode
    HashMap<String, Integer> countMap = new HashMap<>();
    for(int i=0; i<result.length; i++){
      for(int j=0; j<3; j++){
        if(countMap.containsKey(result[i][j])){
          countMap.put(result[i][j], countMap.get(result[i][j]) + 1);
        } else {
          countMap.put(result[i][j], 1);
        }
      }
    }
    for(Map.Entry<String, Integer> entry : countMap.entrySet()){
      System.out.println(entry.getValue());
    }

    // rack for each block
    for(int i=0; i<result.length; i++){
      int[] racks = new int[3];
      for(int j=0; j<3; j++){
        int nodeNum = Integer.parseInt(result[i][j].substring(8));
        if(nodeNum <=5)
          racks[j] = 1;
        else
          racks[j] = 2;
      }
      if(racks[0] == racks[1] && racks[1] == racks[2]){
        System.out.println(i);
        System.out.println(Arrays.toString(result[i]));

      }
    }

    return null;
  }

  public static void main(String[] argvs) throws IOException{
    getAllNodes();

  }
}
