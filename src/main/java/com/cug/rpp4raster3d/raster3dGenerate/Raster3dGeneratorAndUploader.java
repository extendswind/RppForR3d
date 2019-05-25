package com.cug.rpp4raster3d.raster3dGenerate;


import com.cug.rpp4raster3d.raster3d.CellAttrsSimple;
import com.cug.rpp4raster2d.util.SpatialConstant;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;


public class Raster3dGeneratorAndUploader {

  public Raster3dGeneratorAndUploader() {

  }

  /**
   * generate test data in binary format
   * a sizeRow * sizeCol file whose pixel is represented by an Integer of four bytes
   * pixel values are counted from 0
   */
  public static void generateBinaryTestData(String filepath, int xDim, int yDim, int zDim) throws IOException {
    DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(
        new FileOutputStream(new File(filepath))));
    int count = 0;
    for (int z = 0; z < zDim; z++) {
      for (int y = 0; y < yDim; y++) {
        for (int x = 0; x < xDim; x++) {
          CellAttrsSimple cell = new CellAttrsSimple((byte) ((count) % 128));
          cell.write(outputStream);
          count++;
        }
      }
    }
    outputStream.close();
  }

  /**
   * divided the file into rowSplitSize*colSplitSize files
   * warning: fileRowSize should be divided exactly by rowSplitSize, the same to fileColSize
   */
  public static void splitSpatialDataBinary(String filepath, int fileXDim, int fileYDim, int fileZDim,
                                            int xSplitSize, int ySplitSize, int zSplitSize) throws IOException {
    DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(filepath))));
    String filename = FilenameUtils.getName(filepath);
    String uploadFilepath = filepath + "_upload";

    DataOutputStream[] resultWriters = new DataOutputStream[xSplitSize * ySplitSize * zSplitSize];
    FileUtils.forceMkdir(new File(uploadFilepath));

    int cellXDim = fileXDim / xSplitSize;
    int cellYDim = fileYDim / ySplitSize;
    int cellZDim = fileZDim / zSplitSize;

//    FileOutputStream infoFileStream = new FileOutputStream(new File(uploadFilepath +
//        "/info_" + xSplitSize + "_" + ySplitSize + "_" + zSplitSize +
//        cellXDim + "_" + cellYDim + "_" + cellZDim));
//    infoFileStream.close();

    for (int z = 0; z < zSplitSize; z++) {
      for (int y = 0; y < ySplitSize; y++) {
        for (int x = 0; x < xSplitSize; x++) {
          FileOutputStream fileOutputStream = new FileOutputStream(new File(uploadFilepath +
              "/" + SpatialConstant.RASTER_3D_INDEX_PREFIX + "_" + filename + "_" + x + "_" + y + "_" + z));
          resultWriters[z * xSplitSize * ySplitSize + y * xSplitSize + x] =
              new DataOutputStream(new BufferedOutputStream(fileOutputStream));
        }
      }
    }

    CellAttrsSimple tmpCell = new CellAttrsSimple();
    for (int z = 0; z < fileZDim; z++) {
      for (int y = 0; y < fileYDim; y++) {
        for (int x = 0; x < fileXDim; x++) {
          tmpCell.read(inputStream);
          tmpCell.write(
              resultWriters[(z / cellZDim) * xSplitSize * ySplitSize + (y / cellYDim) * xSplitSize + x / cellXDim]);
        }
      }
    }

    inputStream.close();
    for (int i = 0; i < xSplitSize * ySplitSize * zSplitSize; i++) {
      resultWriters[i].close();
    }

  }


  /**
   * upload the split file in *localDirectory* to *hdfsDirectory* , and create a new file named
   * *info_rowSize_colSize_cellRowSize_cellColSize*
   *
   * @param localDirectory local dir, contains cellRowNum * cellColNum file named by the name of localDirectory and
   *                       position
   * @param hdfsDirectory  hdfs dir
  */
  public static void uploadSpatialFile(String localDirectory, String hdfsDirectory,
                                       int cellXDim, int cellYDim, int cellZDim, int xSplitSize, int ySplitSize,
                                       int zSplitSize)
      throws IOException {
    String filename = FilenameUtils.getName(localDirectory).split("_")[0];
    Configuration conf = new Configuration();
    FileSystem hdfs = FileSystem.get(URI.create(hdfsDirectory), conf);

    Path hdfsPath = new Path(hdfsDirectory);
    hdfs.mkdirs(hdfsPath);
    for (int z = 0; z < zSplitSize; z++) {
      for (int y = 0; y < ySplitSize; y++) {
        for (int x = 0; x < xSplitSize; x++) {
          String localFilename = "/" + SpatialConstant.RASTER_3D_INDEX_PREFIX + "_" + filename + "_" + x + "_" + y +
              "_" + z;
          Path localPath = new Path(localDirectory + "/" + localFilename);
          hdfs.copyFromLocalFile(localPath, hdfsPath);
        }
      }
    }

    hdfs.create(new Path(hdfsDirectory + "/info_" +
        xSplitSize + "_" + ySplitSize + "_" + zSplitSize + "_" + cellXDim + "_" + cellYDim + "_" + cellZDim))
        .close();

    hdfs.close();
  }

  //  public static void createInfoFileInHDFS(String hdfsDirectory,
  //                                          int cellRowNum, int cellColNum, int cellRowSize, int cellColSize,
  //                                          int groupRowSize, int groupColSize, int groupRowOverlap, int
  //                                          groupColOverlap)
  //      throws IOException {
  //
  //    Configuration conf = new Configuration();
  //    FileSystem hdfs = FileSystem.get(URI.create(hdfsDirectory), conf);
  //    FSDataOutputStream infoFileStream = hdfs.create(new Path(hdfsDirectory + "/info.dat"));
  //    infoFileStream.writeInt(cellRowNum);
  //    infoFileStream.writeInt(cellColNum);
  //    infoFileStream.writeInt(cellRowSize);
  //    infoFileStream.writeInt(cellColSize);
  //    infoFileStream.writeInt(groupRowSize);
  //    infoFileStream.writeInt(groupColSize);
  //    infoFileStream.writeInt(groupRowOverlap);
  //    infoFileStream.writeInt(groupColOverlap);
  //    infoFileStream.close();
  //
  //
  //    hdfs.close();
  //
  //  }


  public static void main(String[] args) throws IOException {

    System.setProperty("HADOOP_USER_NAME", "sparkl");

    // for every cell use 1 Byte, totally 2.4G
    int modelXDim = 2000;
    int modelYDim = 2000;
    int modelZDim = 600;

    // for every cell use 1 byte, totally 12.5M
    int cellXDim = 250;
    int cellYDim = 250;
    int cellZDim = 200;

    String localFile = "test_data/raster3d-2.4G.dat";

    // upload directory
    String hdfsDir = "hdfs://kvmmaster:9000/user/sparkl/" + FilenameUtils.getName(localFile);

    if (true) {
      generateBinaryTestData(localFile, modelXDim, modelYDim, modelZDim);
      System.out.println("data generate done!");

      splitSpatialDataBinary(localFile, modelXDim, modelYDim, modelZDim, modelXDim/cellXDim,
          modelYDim/cellYDim, modelZDim/cellZDim);
      System.out.println("data split done!");
    }

    if(false) {
      uploadSpatialFile(localFile + "_upload", hdfsDir, cellXDim, cellYDim, cellZDim, modelXDim / cellXDim,
          modelYDim / cellYDim, modelZDim / cellZDim);
      System.out.println("data upload done!");
    }


    // get block location
    //    Configuration conf = new Configuration();
    //    FileSystem fs = FileSystem.get(URI.create("hdfs://hadoopmaster:9000/"), conf);
    //
    //    for(int i=0; i<5; i++){
    //      for(int j=0; j<5; j++){
    //        Path p = new Path("hdfs://hadoopmaster:9000/user/sparkl/test.dat/grid_test.dat_" +
    //                Integer.toString(i) + "_" + Integer.toString(j));
    //        System.out.println(Arrays.toString(fs.getFileBlockLocations(p, 0, 1)));
    //      }
    //    }


    //    Configuration conf = new Configuration();
    //    conf.set("dfs.blocksize", "1048576");
    //    FileSystem hdfs = FileSystem.get(URI.create("hdfs://hadoopmaster:9000/"), conf);
    //
    //    String uploadFile = "testfile.dat";
    //    Path uploadFilePath = new Path(uploadFile);
    //
    //    Path spatialDataPath = new Path("/user/sparkl/spatial_data");
    //    if (!hdfs.exists(spatialDataPath)) {
    //      hdfs.mkdirs(spatialDataPath);
    //    }

    //        hdfs.mkdirs(new Path("hdfs://tdh1:9000/" + FilenameUtils.getBaseName(uploadFile)));
    //
    //        hdfs.delete(new Path("hdfs://tdh1:9000/testData"), true);
    //        hdfs.copyFromLocalFile(false, true,
    //                new Path("/mnt/data/liuzhipeng/test_data/test3mFile"),
    //                new Path("hdfs://tdh1:9000/testData"));


    //    hdfs.close();

  }

}
