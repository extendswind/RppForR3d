package com.cug.geo3d.upload;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.net.URI;

import static com.cug.geo3d.upload.SpatialDataGeneratorAndUploader.generateBinaryTestData;
import static com.cug.geo3d.upload.SpatialDataGeneratorAndUploader.splitSpatialDataBinary;
import static com.cug.geo3d.upload.SpatialDataGeneratorAndUploader.uploadSpatialFile;


public class TestSpatialDataGeneratorAndUploader {


    public TestSpatialDataGeneratorAndUploader() {
    }

    @Test //(timeout = 5000)
    public void generateBinaryTestDataTest() throws IOException {
        generateBinaryTestData("test/test.dat", 1000, 10000);
        DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream("test/test.dat")));
        Assert.assertEquals(inputStream.readInt(), 0);
        inputStream.skip(4);
        Assert.assertEquals(inputStream.readInt(), 2);
        inputStream.skip(40);
        Assert.assertEquals(inputStream.readInt(), 13);
    }

    @Test
    public void splitSpatialDataBinaryTest() throws IOException {
        splitSpatialDataBinary("test/test.dat", 1000, 10000, 5, 5);
    }

//  不需要自动化运行，需要用的时候再调，或者改个名字加备注....
//  @Test(timeout = 5000)
//  public void generateTestDataTest() throws IOException {
//    SpatialDataGeneratorAndUploader.generateTextTestData("test/test.dat", 1000, 10000);
//  }
//
//  @Test
//  public void splitSpatialDataTest() throws IOException {
//    SpatialDataGeneratorAndUploader.splitSpatialData("test/test.dat", 1000, 10000, 5, 5);
//  }

    //@Test
    public void justTest() throws IOException {
        System.setProperty("HADOOP_USER_NAME", "sparkl");

        // grid size 25000*25000, with four bytes a pixel, totally about 2.5G
        int rowSize = 25000;
        int colSize = 25000;
        //generateBinaryTestData("test/big2.dat", rowSize, colSize);
        System.out.println("data generate done!");

        // split into 5*5 file, every file 1000*5000, about 20M
        int cellRowSize = 5000;
        int cellColSize = 1000;
        //splitSpatialDataBinary("test/big2.dat", rowSize, colSize, rowSize / cellRowSize,
        //        colSize / cellColSize);
        System.out.println("data split done!");

        uploadSpatialFile("test/big2.dat_upload",
                "hdfs://kvmmaster:9000/user/sparkl/big2.dat",
                rowSize / cellRowSize, colSize / cellColSize, cellRowSize, cellColSize);
        System.out.println("data upload done!");
//    System.setProperty("HADOOP_USER_NAME", "sparkl");
//      // get block location
//    Configuration conf = new Configuration();
//    FileSystem fs = FileSystem.get(URI.create("hdfs://kvmmaster:9000/"), conf);
//    fs.create(new Path("/test"));
//
//    fs.close();

    }


    // for data upload test
    @Test
    public void testUpload() throws IOException {
        System.setProperty("HADOOP_USER_NAME", "sparkl");

        String filename = "spatial3GNew.dat";
//         String filename = "bigTestFile.dat";

        // grid size 30000*25000, with four bytes a pixel, totally about 3G
        int rowSize = 30000;
        int colSize = 25000;
        //generateBinaryTestData("test/" + filename, rowSize, colSize);
        System.out.println("data generate done!");


        // split into 5*5 file, every file 1000*5000, about 20M
        int cellRowSize = 10000;
        int cellColSize = 500;
        //splitSpatialDataBinary("test/" + filename, rowSize, colSize, rowSize / cellRowSize,
        //        colSize / cellColSize);
        System.out.println("data split done!");


        uploadSpatialFile("test/" + filename + "_upload",
                "hdfs://kvmmaster:9000/notSpatial/" + filename,
                rowSize / cellRowSize, colSize / cellColSize, cellRowSize, cellColSize);
        System.out.println("data upload done!");
    }

}