package com.cug.rpp4raster3d.raster3d;


import java.io.DataInput;
import java.io.IOException;

/**
 * 为了论文实验引入黄土坡测试数据
 */
public class HuangtupoRaster3D extends Raster3D {
    //implements Writable {

  private int xDim;
  private int yDim;
  private int zDim;

  private int attrNum = 5;
  private byte[] attr1;  // stratum;
  private byte[] attr2;  // lithology;
  private float[] attr3;  // moistureContent;
  private float[] attr4;  // hazardIndicator;
  private float[] attr5;  // uncertaintyIndicator;

  // Each voxel contains a stratum type (1 byte), lithology (1 byte), moisture content (1 float),
  // uncertainty indicator (1 float) and hazard indicator (1 float)

//  private byte[] attr;

//  public NormalRaster3D(){
//
//  }

  /**
   * return a large Raster3D consist of multiple small Raster3d, each in a z layer
   * @param raster3ds
   */
  public HuangtupoRaster3D(HuangtupoRaster3D[]  raster3ds){
    this(raster3ds[0].getXDim(), raster3ds[0].getYDim(), raster3ds[0].getZDim());

    int desPos = 0;
    for(int i=0; i<raster3ds.length; i++){
      int layerLength = xDim*yDim*raster3ds[i].getZDim();
      System.arraycopy((raster3ds[i]).attr1, 0, attr1, desPos, layerLength);
      System.arraycopy((raster3ds[i]).attr2, 0, attr2, desPos, layerLength);
      System.arraycopy((raster3ds[i]).attr3, 0, attr3, desPos, layerLength);
      System.arraycopy((raster3ds[i]).attr4, 0, attr4, desPos, layerLength);
      System.arraycopy((raster3ds[i]).attr5, 0, attr5, desPos, layerLength);
      desPos += layerLength;
    }

  }

  // TODO 考虑把3个float加上的难度，还是直接用12个字节
  public HuangtupoRaster3D(int xDim, int yDim, int zDim){
    this.xDim = xDim;
    this.yDim = yDim;
    this.zDim = zDim;
    int size = xDim * yDim * zDim;
    attr1 = new byte[size];
    attr2 = new byte[size];
    attr3 = new float[size];
    attr4 = new float[size];
    attr5 = new float[size];
  }

  public int getXDim(){return xDim; }
  public int getYDim(){return yDim; }
  public int getZDim(){return zDim; }

  public byte[] getStratumArray(){
    return attr1;
  }

  public byte[] getAttr1(){
    return attr1;
  }

//  public SimpleCellAttrs getAttr(int index){
//    return new SimpleCellAttrs(attr[index]);
//  }

  public int getVoxelSize(){
    return 14;
  }

  @Override
  public void upMoveLayerData(int startLayerZ) {
    int startPos = startLayerZ * xDim * yDim;
    int length = xDim*yDim*zDim - startPos;
    System.arraycopy(attr1, startPos, attr1, 0, length);
    System.arraycopy(attr2, startPos, attr2, 0, length);
    System.arraycopy(attr3, startPos, attr3, 0, length);
    System.arraycopy(attr4, startPos, attr4, 0, length);
    System.arraycopy(attr5, startPos, attr5, 0, length);
  }

  @Override
  public void setAttr(int index, VoxelAttrsBase cellattrs) {
    attr1[index] = ((HuangtupoVoxelAttrs) cellattrs).attr1;
    attr2[index] = ((HuangtupoVoxelAttrs) cellattrs).attr2;
    attr3[index] = ((HuangtupoVoxelAttrs) cellattrs).attr3;
    attr4[index] = ((HuangtupoVoxelAttrs) cellattrs).attr4;
    attr5[index] = ((HuangtupoVoxelAttrs) cellattrs).attr5;
  }

  @Override
  public VoxelAttrsBase getAttr(int index) {
    return new HuangtupoVoxelAttrs(attr1[index], attr2[index], attr3[index], attr4[index], attr5[index]);
  }

  @Override
  public HuangtupoRaster3D averageSampling(int radius) {
    int resultXDim = xDim / radius / 2;
    int resultYDim = yDim / radius / 2;
    int resultZDim = zDim / radius / 2;

    HuangtupoRaster3D raster3D = new HuangtupoRaster3D(resultXDim, resultYDim, resultZDim);
    int xstart, ystart, zstart;
    // TODO  貌似右边界的处理有问题，当raster3D的大小不为半径整数倍时右边界可能挂
    for (int i = 0; i < resultZDim; i++) {
      for (int j = 0; j < resultYDim; j++) {
        for (int k = 0; k < resultXDim; k++) {
          xstart = k * radius * 2 + radius;
          ystart = j * radius * 2 + radius;
          zstart = i * radius * 2 + radius;
          long sum1 = 0;
          long sum2 = 0;
          long sum3 = 0;
          long sum4 = 0;
          long sum5 = 0;
          for (int zz = zstart - radius; zz < zstart + radius; zz++) {
            for (int yy = ystart - radius; yy < ystart + radius; yy++) {
              for (int xx = xstart - radius; xx < xstart + radius; xx++) {
                sum1 += attr1[xx + yy * xDim + zz * xDim * yDim];
                sum2 += attr2[xx + yy * xDim + zz * xDim * yDim];
                sum3 += attr3[xx + yy * xDim + zz * xDim * yDim];
                sum4 += attr4[xx + yy * xDim + zz * xDim * yDim];
                sum5 += attr5[xx + yy * xDim + zz * xDim * yDim];
              }
            }
          }
          int surroundSize = (radius * 2) * (radius * 2) * (radius * 2);
          int pos = i * resultXDim * resultYDim + j * resultXDim + k;
          raster3D.attr1[pos] = (byte) ((sum1 / surroundSize) % 128);
          raster3D.attr2[pos] = (byte) ((sum2 / surroundSize) % 128);
          raster3D.attr3[pos] = (byte) ((sum3 / surroundSize) % 128);
          raster3D.attr4[pos] = (byte) ((sum4 / surroundSize) % 128);
          raster3D.attr5[pos] = (byte) ((sum5 / surroundSize) % 128);
        }
      }
    }
    return raster3D;
  }

  @Override
  public Raster3D getZRegion(int zStart, int zEnd) {
    int zHeight = zEnd - zStart;
    HuangtupoRaster3D raster3D = new HuangtupoRaster3D(xDim, yDim, zHeight);
    System.arraycopy(attr1, xDim * yDim * zStart, raster3D.attr1, 0, xDim * yDim * zHeight);
    System.arraycopy(attr2, xDim * yDim * zStart, raster3D.attr2, 0, xDim * yDim * zHeight);
    System.arraycopy(attr3, xDim * yDim * zStart, raster3D.attr3, 0, xDim * yDim * zHeight);
    System.arraycopy(attr4, xDim * yDim * zStart, raster3D.attr4, 0, xDim * yDim * zHeight);
    System.arraycopy(attr5, xDim * yDim * zStart, raster3D.attr5, 0, xDim * yDim * zHeight);
    return raster3D;
  }

  /**
   * read the index cell value from dataInput
   */
  public void readAttr(int index, DataInput dataInput) throws IOException {
    attr1[index] = dataInput.readByte();
    attr2[index] = dataInput.readByte();
    attr3[index] = dataInput.readFloat();
    attr4[index] = dataInput.readFloat();
    attr5[index] = dataInput.readFloat();
  }

//  public void writeAttr(int index, DataOutput dataOutput) throws IOException {
//    dataOutput.writeByte(attr[index]);
//  }
}
