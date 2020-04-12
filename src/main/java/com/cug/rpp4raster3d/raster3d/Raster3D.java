package com.cug.rpp4raster3d.raster3d;


import java.io.DataInput;
import java.io.IOException;

/**
 * 注意体元数量限制在一个int的数值范围内
 * 由于java对象需要占用额外的十几个字节，因此使用对象作为体元会浪费大量的空间
 *
 * TODO 作为value值的序列化
 */
public abstract class Raster3D {

//  public abstract Raster3D(int xDim, int yDim, int zDim);


  public abstract int getXDim();
  public abstract int getYDim();
  public abstract int getZDim();


  public abstract byte[] getAttr0();


  public abstract int getCellSize();

  /**
   * read the cell value from dataInput
   */
  public abstract void readAttr(int index, DataInput dataInput) throws IOException;


  public abstract void setAttr(int index, CellAttrsBase cellattrs);

  public abstract CellAttrsBase getAttr(int index);




  // ----- operation -----

  // used for simple sampling
  public abstract Raster3D averageSampling(int radius);

  public abstract Raster3D getZRegion(int zStart, int zEnd);

  public abstract void upMoveLayerData(int startLayerZ);







  //  public void writeAttr(int index, DataOutput dataOutput) throws IOException {
//    dataOutput.writeByte(attr[index]);
//  }

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



}
