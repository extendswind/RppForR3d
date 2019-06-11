package com.cug.rpp4raster3d.raster3d;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * a test cell attrs, only a byte attribute used
 */
public class CellAttrsSimple extends CellAttrsBase {

  public byte attr;  // 1 byte

  public CellAttrsSimple(){

  }

  public CellAttrsSimple(byte attr){
    this.attr = attr;
  }

  public CellAttrsSimple(DataInput dataInput) throws IOException {
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
