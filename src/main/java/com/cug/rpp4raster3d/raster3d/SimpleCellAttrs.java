package com.cug.rpp4raster3d.raster3d;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * a test cell attrs, only a byte attribute used
 *
 * efficiency problem, using SimpleRaster3D instead
 */
@Deprecated
public class SimpleCellAttrs extends CellAttrsBase {

  public byte attr;  // 1 byte

  public SimpleCellAttrs(){

  }

  public SimpleCellAttrs(byte attr){
    this.attr = attr;
  }

  public SimpleCellAttrs(DataInput dataInput) throws IOException {
    read(dataInput);
  }

  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeByte(attr);
  }

  public void read(DataInput dataInput) throws IOException{
    attr = dataInput.readByte();
  }

  public int getSize(){
    return 1;
  }
}
