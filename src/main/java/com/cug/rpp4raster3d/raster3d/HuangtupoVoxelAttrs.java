package com.cug.rpp4raster3d.raster3d;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * design for HuangtupoRaster3D
 */
public class HuangtupoVoxelAttrs extends VoxelAttrsBase {

  public byte attr1;
  public byte attr2;
  public float attr3;
  public float attr4;
  public float attr5;

  public HuangtupoVoxelAttrs(byte attr1, byte attr2, float attr3, float attr4, float attr5){
    this.attr1 = attr1;
    this.attr2 = attr2;
    this.attr3 = attr3;
    this.attr4 = attr4;
    this.attr5 = attr5;
  }

  @Override
  public void write(DataOutput dataOutputStream) throws IOException {
    dataOutputStream.writeByte(attr1);
    dataOutputStream.writeByte(attr2);
    dataOutputStream.writeFloat(attr3);
    dataOutputStream.writeFloat(attr4);
    dataOutputStream.writeFloat(attr5);
  }

  @Override
  public void read(DataInput dataInputStream) throws IOException{
    attr1 = dataInputStream.readByte();
    attr2 = dataInputStream.readByte();
    attr3 = dataInputStream.readFloat();
    attr4 = dataInputStream.readFloat();
    attr5 = dataInputStream.readFloat();
  }

  @Override
  public int getSize() {
    return 14;
  }
}
