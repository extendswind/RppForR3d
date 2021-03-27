package com.cug.rpp4raster3d.spatialInputFormat;


import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * 直接在InputFormat中统计IO的开销
 *
 */
public class IOStatics {

  private String localNode;
  private int splitId;
  private FileSystem fs;
  private int voxelSize;

  private double remoteIO;
  private double totalIO;

  public IOStatics(String localNode, int splitId, FileSystem fs, int voxelSize) {
    this.localNode = localNode;
    this.splitId = splitId;
    this.fs = fs;
    this.voxelSize = voxelSize;
    remoteIO = 0;
    totalIO = 0;
  }

  public void addRemoteReadIO(Path path, int lengthX, int lengthY, int lengthZ) {
    // 判断是否为local
    try {
      BlockLocation[] blockLocations = fs.getFileBlockLocations(path, 0, 1);
      String[] blockHosts = blockLocations[0].getHosts();
      boolean isLocal = false;
      for(String blockHost: blockHosts){
        if(blockHost.equals(localNode)){
          isLocal = true;
        }
      }
      double ioData = lengthX * lengthY * lengthZ * voxelSize / 1024.0 / 1024.0 / 1024.0;
      if(!isLocal){
        remoteIO += ioData;
      }
      totalIO += ioData;
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void writeTofile() {
    // write to a local file
    try {
      File file = new File("/tmp/rpp_iostats_theory");
      if (!file.isFile()) {
        file.createNewFile();
        FileOutputStream fileOutputStream = null;
        fileOutputStream = new FileOutputStream(file, true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
        Date date = new Date();
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd|HH:mm");
        bufferedWriter.write(dateFormat.format(date) + " " + splitId + " " + totalIO + "G " +
            remoteIO + "G\n");
        bufferedWriter.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
