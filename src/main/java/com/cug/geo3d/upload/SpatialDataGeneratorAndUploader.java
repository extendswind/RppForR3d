package com.cug.geo3d.upload;


import org.apache.commons.compress.compressors.FileNameUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

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
   * @param filepath the file to be split
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
            "/info_" + Integer.toString(rowSplitSize) + "_" + Integer.toString(colSplitSize) + "_" +
            Integer.toString(cellRowSize) + "_" + Integer.toString(cellColSize)));
    infoFileStream.close();

    for (int i = 0; i < rowSplitSize; i++) {
      for (int j = 0; j < colSplitSize; j++) {
        FileOutputStream fileOutputStream = new FileOutputStream(new File(uploadFilepath +
                "/grid_" + filename + "_" + Integer.toString(i) + "_" + Integer.toString(j)));
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
   * generate test data in binary format
   * a sizeRow * sizeCol file whose pixel is represented by an Integer of four bytes
   * pixel values are counted from 0
   *
   * @param filepath output file path
   * @param sizeRow row size
   * @param sizeCol col size
   * @throws IOException
   */
  public static void generateTestData(String filepath, int sizeRow, int sizeCol) throws IOException {
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
   * @param filepath the file to be split
   * @param fileRowSize  row size of the split file
   * @param fileColSize  column size of the split file
   * @param rowSplitSize split to rowSize * colSize
   * @param colSplitSize split to rowSize * colSize
   * @throws IOException
   */
  public static void splitSpatialData(String filepath, int fileRowSize, int fileColSize, int rowSplitSize, int colSplitSize) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(filepath))));
    String filename = FilenameUtils.getName(filepath);
    String uploadFilepath = filepath + "_upload";

    BufferedWriter[] resultWriters = new BufferedWriter[rowSplitSize * colSplitSize];
    FileUtils.forceMkdir(new File(uploadFilepath));

    for (int i = 0; i < rowSplitSize; i++) {
      for (int j = 0; j < colSplitSize; j++) {
        FileOutputStream fileOutputStream = new FileOutputStream(new File(uploadFilepath +
                "/grid_" + filename + "_" + Integer.toString(i) + "_" + Integer.toString(j)));
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
   * @param localDirectory local dir, contains rowSize * colSize file named by the name of localDirectory and position
   * @param hdfsDirectory hdfs dir
   * @param rowSize  specified in {@link SpatialDataGeneratorAndUploader#splitSpatialDataBinary}
   * @param colSize idem
   * @param cellRowSize  idem
   * @param cellColSize  idem
   */
  public static void uploadSpatialFile(String localDirectory, String hdfsDirectory,
                                       int rowSize, int colSize, int cellRowSize, int cellColSize)
          throws IOException {
    String filename = FilenameUtils.getName(localDirectory).split("_")[0];
    Configuration conf = new Configuration();
    FileSystem hdfs = FileSystem.get(URI.create(hdfsDirectory), conf);

    Path hdfsPath = new Path(hdfsDirectory);
    hdfs.mkdirs(hdfsPath);
    for (int i = 0; i < rowSize; i++) {
      for (int j = 0; j < colSize; j++) {
        String localFilename = "grid_" + filename + "_" + Integer.toString(i) + "_" + Integer.toString(j);
        Path localPath = new Path(localDirectory + "/" + localFilename);
        hdfs.copyFromLocalFile(localPath, hdfsPath);
      }
    }
    hdfs.create(new Path(hdfsDirectory +
            "/info_" + Integer.toString(rowSize) + "_" + Integer.toString(colSize)+ "_" +
            Integer.toString(cellRowSize)+ "_" + Integer.toString(cellColSize))).close();
    hdfs.close();
  }


  public static void main(String[] args) throws IOException {

    System.setProperty("HADOOP_USER_NAME", "sparkl");

    // grid size 25000*25000, with four bytes a pixel, totally about 2.5G
    int rowSize = 25000;
    int colSize = 25000;
//    generateBinaryTestData("test/big2.dat", rowSize, colSize);
    System.out.println("data generate done!");

    // split into 5*5 file, every file 1000*5000, about 20M
    int cellRowSize = 5000;
    int cellColSize = 1000;
//    splitSpatialDataBinary("test/big2.dat", rowSize, colSize, rowSize/cellRowSize,
 //           colSize/cellColSize);
    System.out.println("data split done!");

    uploadSpatialFile("test/big2.dat_upload",
            "hdfs://kvmmaster:9000/user/sparkl/big2.dat",
            rowSize/cellRowSize, colSize/cellColSize, cellRowSize, cellColSize);
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
