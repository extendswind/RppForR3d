package com.cug.geo3d.upload;

import org.junit.Assert;
import org.junit.Test;

import java.io.*;


public class TestSpatialMapUploader {

  private SpatialMapUploader spatialMapUploader;

  public TestSpatialMapUploader() {
    spatialMapUploader = new SpatialMapUploader();
  }

  @Test(timeout = 5000)
  public void generateBinaryTestDataTest() throws IOException {
    SpatialMapUploader.generateBinaryTestData("test/test.dat", 1000, 10000);
    DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream("test/test.dat")));
    Assert.assertEquals(inputStream.readInt(), 0);
    inputStream.skip(4);
    Assert.assertEquals(inputStream.readInt(), 2);
    inputStream.skip(40);
    Assert.assertEquals(inputStream.readInt(), 13);
  }

  @Test
  public void splitSpatialDataBinaryTest() throws IOException {
    SpatialMapUploader.splitSpatialDataBinary("test/test.dat", 1000, 10000, 5, 5);
  }


  @Test(timeout = 5000)
  public void generateTestDataTest() throws IOException {
    SpatialMapUploader.generateTestData("test/test.dat", 1000, 10000);
  }

  @Test
  public void splitSpatialDataTest() throws IOException {
    SpatialMapUploader.splitSpatialData("test/test.dat", 1000, 10000, 5, 5);
  }

  @Test
  public void justTest() throws IOException {
  }

}