package com.cug.rpp4raster3d.raster3d;


import java.io.DataInput;
import java.io.IOException;

/**
 * 注意体元数量限制在一个int的数值范围内
 *
 * TODO 作为value值的序列化
 */
public abstract class Raster3D {

//  public abstract Raster3D(int xDim, int yDim, int zDim);

  public abstract int getXDim();
  public abstract int getYDim();
  public abstract int getZDim();

  public void setAttr(int index){

  }

  public abstract byte[] getAttr0();

//  public CellAttrsSimple getAttr(int index){
//    return new CellAttrsSimple(attr1[index]);
//  }

  public abstract int getCellSize();

  /**
   * read the cell value from dataInput
   */
  public abstract void readAttr(int index, DataInput dataInput) throws IOException;


  //  public void writeAttr(int index, DataOutput dataOutput) throws IOException {
//    dataOutput.writeByte(attr1[index]);
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
//      dataOutput.writeByte(attr1[i]);
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
//      attr1[i] = in.readByte();
//    }
//  }



}
