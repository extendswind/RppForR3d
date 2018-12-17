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


public class SpatialMapUploader {

  public SpatialMapUploader() {

  }

  public static void generateBinaryTestData(String filepath, int sizeX, int sizeY) throws IOException {
    DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(filepath))));
    for (int i = 0; i < sizeY; i++) {
      for (int j = 0; j < sizeX; j++) {
        outputStream.writeInt(j + i * sizeX);
      }
    }
    outputStream.close();
  }

  /**
   * divided the file into rowSize*colSize files
   * warning: fileRowSize should be divided exactly by rowSize, the same to fileColSize
   *
   * @param filepath
   * @param fileRowSize
   * @param fileColSize
   * @param rowSize
   * @param colSize
   * @throws IOException
   */
  public static void splitSpatialDataBinary(String filepath, int fileRowSize, int fileColSize, int rowSize, int colSize) throws IOException {
    DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(filepath))));
    String filename = FilenameUtils.getName(filepath);
    String uploadFilepath = filepath + "_upload";

    DataOutputStream[] resultWriters = new DataOutputStream[rowSize * colSize];
    FileUtils.forceMkdir(new File(uploadFilepath));

    int cellRowSize = fileRowSize / rowSize;
    int cellColSize = fileColSize / colSize;
    FileOutputStream infoFileStream = new FileOutputStream(new File(uploadFilepath +
            "/info_" + Integer.toString(rowSize) + "_" + Integer.toString(colSize) + "_" +
            Integer.toString(cellRowSize) + "_" + Integer.toString(cellColSize)));
    infoFileStream.close();

    for (int i = 0; i < rowSize; i++) {
      for (int j = 0; j < colSize; j++) {
        FileOutputStream fileOutputStream = new FileOutputStream(new File(uploadFilepath +
                "/grid_" + filename + "_" + Integer.toString(i) + "_" + Integer.toString(j)));
        resultWriters[i * colSize + j] = new DataOutputStream(new BufferedOutputStream(fileOutputStream));
      }
    }

    int blockWidth = fileColSize / colSize;
    int blockHeight = fileRowSize / rowSize;

    int nextInt;
    for (int i = 0; i < fileRowSize; i++) {
      for (int j = 0; j < fileColSize; j++) {
        nextInt = inputStream.readInt();
        int writerId = (i / blockHeight) * colSize + (j / blockWidth);
        resultWriters[writerId].writeInt(nextInt);
      }
    }

    inputStream.close();
    for (int i = 0; i < rowSize * colSize; i++) {
      resultWriters[i].close();
    }

  }


  public static void generateTestData(String filename, int sizeX, int sizeY) throws IOException {
    FileOutputStream fileOutputStream = new FileOutputStream(new File(filename));
    BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));

    for (int i = 0; i < sizeY; i++) {
      for (int j = 0; j < sizeX; j++) {
        bufferedWriter.write(Integer.toString(j + i * sizeX));
        bufferedWriter.write("\n");
      }
    }
    bufferedWriter.flush();
    bufferedWriter.close();
  }

  public static void splitSpatialData(String filepath, int fileRow, int fileCol, int row, int col) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(filepath))));
    String filename = FilenameUtils.getName(filepath);
    String uploadFilepath = filepath + "_upload";

    BufferedWriter[] resultWriters = new BufferedWriter[row * col];
    FileUtils.forceMkdir(new File(uploadFilepath));

    for (int i = 0; i < row; i++) {
      for (int j = 0; j < col; j++) {
        FileOutputStream fileOutputStream = new FileOutputStream(new File(uploadFilepath +
                "/grid_" + filename + "_" + Integer.toString(i) + "_" + Integer.toString(j)));
        resultWriters[i * col + j] = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
      }
    }

    int blockWidth = fileCol / col;
    int blockHeight = fileRow / row;

    String lineText = null;
    for (int i = 0; i < fileRow; i++) {
      for (int j = 0; j < fileCol; j++) {
        lineText = br.readLine();
        int writerid = (i / blockHeight) * col + (j / blockWidth);
        resultWriters[writerid].write(lineText + "\n");
      }
    }

    br.close();
    for (int i = 0; i < row * col; i++) {
      resultWriters[i].flush();
      resultWriters[i].close();
    }
  }

  // TODO 新建文件info_rowSize_colSize_cellRowSize_cellColSize用于保存文件的基本空间信息
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

    // upload split file to the cluster
    uploadSpatialFile("test/test.dat_upload",
            "hdfs://hadoopmaster:9000/user/sparkl/test.dat",
            5, 5, 200, 2000);

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create("hdfs://hadoopmaster:9000/"), conf);

    for(int i=0; i<5; i++){
      for(int j=0; j<5; j++){
        Path p = new Path("hdfs://hadoopmaster:9000/user/sparkl/test.dat/grid_test.dat_" +
                Integer.toString(i) + "_" + Integer.toString(j));
        System.out.println(Arrays.toString(fs.getFileBlockLocations(p, 0, 1)));
      }
    }





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
