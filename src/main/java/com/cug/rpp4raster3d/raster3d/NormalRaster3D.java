package com.cug.rpp4raster3d.raster3d;


import java.io.DataInput;
import java.io.IOException;

/**
 * 注意体元数量限制在一个int的数值范围内
 */
public class NormalRaster3D extends Raster3D {
    //implements Writable {

  private int xDim;
  private int yDim;
  private int zDim;

  private int attrNum = 10;
  private byte[][] attrArrays;
//  private byte[] attr;

//  public NormalRaster3D(){
//
//  }

  /**
   * return a large Raster3D consist of multiple small Raster3d, each in a z layer
   * @param raster3ds
   */
  public NormalRaster3D(NormalRaster3D[]  raster3ds){
    xDim = raster3ds[0].getXDim();
    yDim = raster3ds[0].getYDim();
    zDim = raster3ds[0].getZDim() * raster3ds.length;
    attrArrays = new byte[attrNum][];
    for(int attrId = 0; attrId < attrNum; attrId++){
      attrArrays[attrId] = new byte[xDim*yDim*zDim];
      for(int i=0; i<raster3ds.length; i++){
        System.arraycopy((raster3ds[i]).attrArrays[attrId], 0, attrArrays[attrId], i*xDim*yDim*raster3ds[0].getZDim(),
            xDim*yDim*raster3ds[0].getZDim());
      }
    }

  }

  public NormalRaster3D(int xDim, int yDim, int zDim){
    this.xDim = xDim;
    this.yDim = yDim;
    this.zDim = zDim;
    attrArrays = new byte[attrNum][];
    for(int i = 0; i < attrNum; i++){
      attrArrays[i] = new byte[xDim*yDim*zDim];
    }
  }

  public int getXDim(){return xDim; }
  public int getYDim(){return yDim; }
  public int getZDim(){return zDim; }

  public void setAttr(int index){

  }

  public byte[] getAttr0(){
    return attrArrays[0];
  }

//  public SimpleCellAttrs getAttr(int index){
//    return new SimpleCellAttrs(attr[index]);
//  }

  public int getCellSize(){
    return 10;
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

  @Override
  public void upMoveLayerData(int startLayerZ) {
    int startPos = startLayerZ * xDim * yDim;
    int length = xDim*yDim*zDim - startPos;
    for(int i=0; i<attrNum; i++) {
      System.arraycopy(attrArrays[i], startPos, attrArrays[i], 0, length);
    }
  }

  @Override
  public void setAttr(int index, CellAttrsBase cellattrs) {
    for(int i=0; i<attrNum; i++) {
      attrArrays[i][index] = ((NormalCellAttrs) cellattrs).attrs[i];
    }
  }

  @Override
  public CellAttrsBase getAttr(int index) {
    return new NormalCellAttrs(attrArrays[index]);
  }

  @Override
  public NormalRaster3D averageSampling(int radius) {
    int resultXDim = xDim / radius / 2;
    int resultYDim = yDim / radius / 2;
    int resultZDim = zDim / radius / 2;

    NormalRaster3D raster3D = new NormalRaster3D(resultXDim, resultYDim, resultZDim);
    int xstart, ystart, zstart;
    NormalCellAttrs cell = new NormalCellAttrs(attrArrays[0]);
    for (int i = 0; i < resultZDim; i++) {
      for (int j = 0; j < resultYDim; j++) {
        for (int k = 0; k < resultXDim; k++) {
          for(int attrId = 0; attrId < attrNum; attrId++) {
            xstart = k * radius * 2 + radius;
            ystart = j * radius * 2 + radius;
            zstart = i * radius * 2 + radius;
            long sum = 0;
            for (int zz = zstart - radius; zz < zstart + radius; zz++) {
              for (int yy = ystart - radius; yy < ystart + radius; yy++) {
                for (int xx = xstart - radius; xx < xstart + radius; xx++) {
                  sum += attrArrays[i][xx + yy * xDim + zz * xDim * yDim];
                }
              }
            }
            cell.attrs[attrId] = (byte) ((sum / (radius * 2) / (radius * 2) / (radius * 2)) % 128);
          }
          raster3D.setAttr(i * resultXDim * resultYDim + j * resultXDim + k, cell);
        }
      }
    }
    return raster3D;
  }

  @Override
  public Raster3D getZRegion(int zStart, int zEnd) {
    int zHeight = zEnd - zStart;
    NormalRaster3D raster3D = new NormalRaster3D(xDim, yDim, zHeight);
    for(int i=0; i<attrNum; i++) {
      System.arraycopy(attrArrays[i], xDim * yDim * zStart, raster3D.attrArrays[i], 0, xDim * yDim * zHeight);
    }
    return raster3D;
  }

  /**
   * read the index cell value from dataInput
   */
  public void readAttr(int index, DataInput dataInput) throws IOException {
    for(int i=0; i<attrNum; i++){
      attrArrays[i][index] = dataInput.readByte();
    }
  }

//  public void writeAttr(int index, DataOutput dataOutput) throws IOException {
//    dataOutput.writeByte(attr[index]);
//  }
}
