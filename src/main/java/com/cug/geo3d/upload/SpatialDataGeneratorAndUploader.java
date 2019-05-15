package com.cug.geo3d.upload;


import org.apache.commons.compress.compressors.FileNameUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.util.Arrays;


public class SpatialDataGeneratorAndUploader {

  public SpatialDataGeneratorAndUploader() {

  }

  /**
   * generate test data in binary format
   * a sizeRow * sizeCol file whose pixel is represented by an Integer of four bytes
   * pixel values are counted from 0
   *
   * @param filepath output file path
   * @param sizeRow  row size of the file
   * @param sizeCol  column size of the file
   * @throws IOException
   */
  public static void generateBinaryTestData(String filepath, int sizeRow, int sizeCol) throws IOException {
    DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(
        new FileOutputStream(new File(filepath))));
    for (int i = 0; i < sizeCol; i++) {
      for (int j = 0; j < sizeRow; j++) {
        outputStream.writeInt(j + i * sizeRow);
      }
    }
    outputStream.close();
  }

  /**
   * divided the file into rowSplitSize*colSplitSize files
   * warning: fileRowSize should be divided exactly by rowSplitSize, the same to fileColSize
   *
   * @param filepath     the file to be split
   * @param fileRowSize  row size of the split file
   * @param fileColSize  column size of the split file
   * @param rowSplitSize split to rowSplitSize * colSplitSize
   * @param colSplitSize split to rowSplitSize * colSplitSize
   * @throws IOException
   */
  public static void splitSpatialDataBinary(String filepath, int fileRowSize, int fileColSize, int rowSplitSize,
                                            int colSplitSize) throws IOException {
    DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(filepath))));
    String filename = FilenameUtils.getName(filepath);
    String uploadFilepath = filepath + "_upload";

    DataOutputStream[] resultWriters = new DataOutputStream[rowSplitSize * colSplitSize];
    FileUtils.forceMkdir(new File(uploadFilepath));

    int cellRowSize = fileRowSize / rowSplitSize;
    int cellColSize = fileColSize / colSplitSize;


    FileOutputStream infoFileStream = new FileOutputStream(new File(uploadFilepath +
        "/info_" + rowSplitSize + "_" + colSplitSize + "_" +
        cellRowSize + "_" + cellColSize));
    infoFileStream.close();

    for (int i = 0; i < rowSplitSize; i++) {
      for (int j = 0; j < colSplitSize; j++) {
        FileOutputStream fileOutputStream = new FileOutputStream(new File(uploadFilepath +
            "/grid_" + filename + "_" + i + "_" + j));
        resultWriters[i * colSplitSize + j] = new DataOutputStream(new BufferedOutputStream(fileOutputStream));
      }
    }

    int blockWidth = fileColSize / colSplitSize;
    int blockHeight = fileRowSize / rowSplitSize;

    int nextInt;
    for (int i = 0; i < fileRowSize; i++) {
      for (int j = 0; j < fileColSize; j++) {
        nextInt = inputStream.readInt();
        int writerId = (i / blockHeight) * colSplitSize + (j / blockWidth);
        resultWriters[writerId].writeInt(nextInt);
      }
    }

    inputStream.close();
    for (int i = 0; i < rowSplitSize * colSplitSize; i++) {
      resultWriters[i].close();
    }

  }


  /**
   * generate test data in text format
   * a sizeRow * sizeCol file whose pixel is represented by an Integer of four bytes
   * pixel values are counted from 0
   *
   * @param filepath output file path
   * @param sizeRow  row size
   * @param sizeCol  col size
   * @throws IOException
   */
  public static void generateTextTestData(String filepath, int sizeRow, int sizeCol) throws IOException {
    FileOutputStream fileOutputStream = new FileOutputStream(new File(filepath));
    BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));

    for (int i = 0; i < sizeCol; i++) {
      for (int j = 0; j < sizeRow; j++) {
        bufferedWriter.write(Integer.toString(j + i * sizeRow));
        bufferedWriter.write("\n");
      }
    }
    bufferedWriter.flush();
    bufferedWriter.close();
  }

  /**
   * divided the file into rowSize*colSize files for text file
   * warning: fileRowSize should be divided exactly by rowSize, the same to fileColSize
   *
   * @param filepath     the file to be split
   * @param fileRowSize  row size of the split file
   * @param fileColSize  column size of the split file
   * @param rowSplitSize split to rowSize * colSize
   * @param colSplitSize split to rowSize * colSize
   * @throws IOException
   */
  public static void splitSpatialData(String filepath, int fileRowSize, int fileColSize, int rowSplitSize,
                                      int colSplitSize) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(filepath))));
    String filename = FilenameUtils.getName(filepath);
    String uploadFilepath = filepath + "_upload";

    BufferedWriter[] resultWriters = new BufferedWriter[rowSplitSize * colSplitSize];
    FileUtils.forceMkdir(new File(uploadFilepath));

    for (int i = 0; i < rowSplitSize; i++) {
      for (int j = 0; j < colSplitSize; j++) {
        FileOutputStream fileOutputStream = new FileOutputStream(new File(uploadFilepath +
            "/grid_" + filename + "_" + i + "_" + j));
        resultWriters[i * colSplitSize + j] = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
      }
    }

    int blockWidth = fileColSize / colSplitSize;
    int blockHeight = fileRowSize / rowSplitSize;

    String lineText = null;
    for (int i = 0; i < fileRowSize; i++) {
      for (int j = 0; j < fileColSize; j++) {
        lineText = br.readLine();
        int writerid = (i / blockHeight) * colSplitSize + (j / blockWidth);
        resultWriters[writerid].write(lineText + "\n");
      }
    }

    br.close();
    for (int i = 0; i < rowSplitSize * colSplitSize; i++) {
      resultWriters[i].flush();
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
   * @param cellRowNum     specified in {@link SpatialDataGeneratorAndUploader#splitSpatialDataBinary}
   * @param cellColNum     idem
   * @param cellRowSize    idem
   * @param cellColSize    idem
   */
  public static void uploadSpatialFile(String localDirectory, String hdfsDirectory,
                                       int cellRowNum, int cellColNum, int cellRowSize, int cellColSize)
      throws IOException {
    String filename = FilenameUtils.getName(localDirectory).split("_")[0];
    Configuration conf = new Configuration();
    FileSystem hdfs = FileSystem.get(URI.create(hdfsDirectory), conf);

    Path hdfsPath = new Path(hdfsDirectory);
    hdfs.mkdirs(hdfsPath);
    for (int i = 0; i < cellRowNum; i++) {
      for (int j = 0; j < cellColNum; j++) {
        String localFilename = "grid_" + filename + "_" + i + "_" + j;
        Path localPath = new Path(localDirectory + "/" + localFilename);
        hdfs.copyFromLocalFile(localPath, hdfsPath);
      }
    }

    hdfs.create( new Path(hdfsDirectory + "/info_" +
        cellRowNum + "_" + cellColNum + "_" + cellRowSize + "_" + cellColSize))
        .close();

    hdfs.close();
  }

  public static void createInfoFileInHDFS(String hdfsDirectory,
                                          int cellRowNum, int cellColNum, int cellRowSize, int cellColSize,
                                          int groupRowSize, int groupColSize, int groupRowOverlap, int groupColOverlap)
      throws IOException {

    Configuration conf = new Configuration();
    FileSystem hdfs = FileSystem.get(URI.create(hdfsDirectory), conf);
    FSDataOutputStream infoFileStream = hdfs.create(new Path(hdfsDirectory + "/info.dat"));
    infoFileStream.writeInt(cellRowNum);
    infoFileStream.writeInt(cellColNum);
    infoFileStream.writeInt(cellRowSize);
    infoFileStream.writeInt(cellColSize);
    infoFileStream.writeInt(groupRowSize);
    infoFileStream.writeInt(groupColSize);
    infoFileStream.writeInt(groupRowOverlap);
    infoFileStream.writeInt(groupColOverlap);
    infoFileStream.close();


    hdfs.close();

  }

  //  public static void addGroupInfo2Filename(String filename, grou)


  public static void main(String[] args) throws IOException {

    System.setProperty("HADOOP_USER_NAME", "sparkl");

    // grid size 27000*27000, with four bytes a pixel, totally about 2.78G
    int gridRowSize = 27000;
    int gridColSize = 27000;
    String localFile = "test_data/test-3G-9-9.dat";

    // split into 9*9 file, every file 3000*3000, about 35M
    // 3*3 groups
    int cellRowSize = 3000;
    int cellColSize = 3000;

    // upload directory
    String hdfsDir = "hdfs://kvmmaster:9000/user/sparkl/" + FilenameUtils.getName(localFile);

    // group info    rowOverlapSize use default 1
    int groupRowSize = 3;
    int groupColSize = 3;


    if (true) {
      generateBinaryTestData(localFile, gridRowSize, gridColSize);
      System.out.println("data generate done!");

      splitSpatialDataBinary(localFile, gridRowSize, gridColSize, gridRowSize / cellRowSize,
          gridColSize / cellColSize);
      System.out.println("data split done!");

      createInfoFileInHDFS(hdfsDir, gridRowSize / cellRowSize, gridColSize / cellColSize,
          cellRowSize, cellColSize, groupRowSize, groupColSize, 1, 1);
    }

    uploadSpatialFile(localFile + "_upload", hdfsDir,
        gridRowSize / cellRowSize, gridColSize / cellColSize, cellRowSize, cellColSize);
    System.out.println("data upload done!");


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
