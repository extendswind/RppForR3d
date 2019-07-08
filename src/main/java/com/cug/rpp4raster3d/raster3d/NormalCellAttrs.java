package com.cug.rpp4raster3d.raster3d;

import java.io.*;


/**
 * design for NormalRaster3D
 */
public class NormalCellAttrs extends CellAttrsBase {

  public byte[] attrs;

  public NormalCellAttrs(byte[] attrs){
    this.attrs = new byte[attrs.length];
    System.arraycopy(attrs, 0, this.attrs, 0, attrs.length);
  }

  @Override
  public void write(DataOutput dataOutputStream) throws IOException {
    dataOutputStream.writeInt(attrs.length);
    for(byte attr:attrs){
      dataOutputStream.writeByte(attr);
    }
  }

  @Override
  public void read(DataInput dataInputStream) throws IOException{
    int length = dataInputStream.readInt();
    attrs = new byte[length];
    for(int i=0; i<length; i++){
      attrs[i] = dataInputStream.readByte();
    }
  }

  @Override
  public int getSize() {
    return 10;
  }
}
