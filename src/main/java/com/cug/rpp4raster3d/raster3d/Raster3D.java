package com.cug.rpp4raster3d.raster3d;


import com.google.common.collect.Table;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 注意体元数量限制在一个int的数值范围内
 */
public class Raster3D implements Writable {

  private int xDim;
  private int yDim;
  private int zDim;

  private byte[] attr1;

  public Raster3D(){

  }

  public Raster3D(int xDim, int yDim, int zDim){
    this.xDim = xDim;
    this.yDim = yDim;
    this.zDim = zDim;
    attr1 = new byte[xDim*yDim*zDim];
  }

  public int getXDim(){return xDim; }
  public int getYDim(){return yDim; }
  public int getZDim(){return zDim;}

  public void setAttr(int index){

  }

  public byte[] getAttr1(){
    return attr1;
  }

//  public CellAttrsSimple getAttr(int index){
//    return new CellAttrsSimple(attr1[index]);
//  }

  public int getCellSize(){
    return 1;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(xDim);
    dataOutput.writeInt(yDim);
    dataOutput.writeInt(zDim);
    for(int i=0; i<xDim*yDim*zDim; i++){
      dataOutput.writeByte(attr1[i]);
    }

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    xDim = in.readInt();
    yDim = in.readInt();
    zDim = in.readInt();
    for(int i=0; i<xDim*yDim*zDim; i++){
      attr1[i] = in.readByte();
    }
  }

  public void readAttr(int index, DataInput dataInput) throws IOException {
    attr1[index] = dataInput.readByte();
  }

  public void writeAttr(int index, DataOutput dataOutput) throws IOException {
    dataOutput.writeByte(attr1[index]);
  }
}
