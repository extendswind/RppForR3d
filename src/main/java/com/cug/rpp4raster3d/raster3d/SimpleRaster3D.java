package com.cug.rpp4raster3d.raster3d;


import java.io.DataInput;
import java.io.IOException;

/**
 * 注意体元数量限制在一个int的数值范围内
 */
public class SimpleRaster3D extends Raster3D {

  private int xDim;
  private int yDim;
  private int zDim;

  byte[] attr;

  /**
   * merge multiple layer to one Raster3D
   * @param raster3ds
   */
  public SimpleRaster3D(SimpleRaster3D[] raster3ds){
    xDim = raster3ds[0].getXDim();
    yDim = raster3ds[0].getYDim();
    zDim = raster3ds[0].getZDim() * raster3ds.length;
    attr = new byte[xDim*yDim*zDim];
    for(int i=0; i<raster3ds.length; i++){
      System.arraycopy((raster3ds[i]).attr, 0, attr, i*xDim*yDim*raster3ds[0].getZDim(),
          xDim*yDim*raster3ds[0].getZDim());
    }
  }

//  public SimpleRaster3D() {
//
//  }

  public SimpleRaster3D(int xDim, int yDim, int zDim) {
    this.xDim = xDim;
    this.yDim = yDim;
    this.zDim = zDim;
    attr = new byte[xDim * yDim * zDim];
  }

  public int getXDim() {return xDim; }

  public int getYDim() {return yDim; }

  public int getZDim() {return zDim; }

  @Override
  public byte[] getAttr1() {
    return attr;
  }

  public void setAttr(int index, VoxelAttrsBase cell) {
    attr[index] = ((SimpleVoxelAttrs) cell).attr;
  }

  @Override
  public VoxelAttrsBase getAttr(int index) {
    return new SimpleVoxelAttrs(attr[index]);
  }
//
//  public byte[] getAttr0() {
//    return attr;
//  }

  //  public SimpleCellAttrs getAttr(int index){
  //    return new SimpleCellAttrs(attr[index]);
  //  }

  public int getVoxelSize() {
    return 1;
  }

  /**
   * 用于序列化
   */
  //  @Override
  //  public void write(DataOutput dataOutput) throws IOException {
  //    dataOutput.writeInt(xDim);
  //    dataOutput.writeInt(yDim);
  //    dataOutput.writeInt(zDim);
  //    for(int i=0; i<xDim*yDim*zDim; i++){
  //      dataOutput.writeByte(attr[i]);
  //    }
  //  }

  /**
   * 用于序列化
   */
  //  @Override
  //  public void readFields(DataInput in) throws IOException {
  //    xDim = in.readInt();
  //    yDim = in.readInt();
  //    zDim = in.readInt();
  //    for(int i=0; i<xDim*yDim*zDim; i++){
  //      attr[i] = in.readByte();
  //    }
  //  }

  /**
   * read the index cell value from dataInput
   */
  public void readAttr(int index, DataInput dataInput) throws IOException {
    attr[index] = dataInput.readByte();
  }



  // -------- operation ---------
  @Override
  public void upMoveLayerData(int startLayerZ) {
    int startPos = startLayerZ * xDim * yDim;
    int length = xDim * yDim * zDim - startPos;
    System.arraycopy(attr, startPos, attr, 0, length);
  }

  @Override
  public SimpleRaster3D averageSampling(int radius) {
    int resultXDim = xDim / radius / 2;
    int resultYDim = yDim / radius / 2;
    int resultZDim = zDim / radius / 2;
    SimpleRaster3D raster3D = new SimpleRaster3D(resultXDim, resultYDim, resultZDim);
    SimpleVoxelAttrs cell = new SimpleVoxelAttrs();
    int xstart, ystart, zstart;
    for (int i = 0; i < resultZDim; i++) {
      for (int j = 0; j < resultYDim; j++) {
        for (int k = 0; k < resultXDim; k++) {
          xstart = k * radius * 2 + radius;
          ystart = j * radius * 2 + radius;
          zstart = i * radius * 2 + radius;
          long sum = 0;
          for (int zz = zstart - radius; zz < zstart + radius; zz++) {
            for (int yy = ystart - radius; yy < ystart + radius; yy++) {
              for (int xx = xstart - radius; xx < xstart + radius; xx++) {
                sum += attr[xx + yy * xDim + zz * xDim * yDim];
              }
            }
          }
          cell.attr = (byte) ((sum / (radius * 2) / (radius * 2) / (radius * 2)) % 128);
          raster3D.setAttr(i * resultXDim * resultYDim + j * resultXDim + k, cell);
        }
      }
    }

    return raster3D;
  }

  @Override
  public Raster3D getZRegion(int zStart, int zEnd) {
    int zHeight = zEnd - zStart;
    SimpleRaster3D raster3D = new SimpleRaster3D(xDim, yDim, zHeight);
    System.arraycopy(attr, xDim*yDim*zStart, raster3D.attr, 0, xDim*yDim*zHeight);
    return raster3D;
  }


  //  public void writeAttr(int index, DataOutput dataOutput) throws IOException {
  //    dataOutput.writeByte(attr[index]);
  //  }
}
