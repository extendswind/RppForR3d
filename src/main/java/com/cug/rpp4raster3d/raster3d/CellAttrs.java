package com.cug.rpp4raster3d.raster3d;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class CellAttrs {
int attr1;
int attr2;
char attr3;

  public void write(DataOutputStream dataOutputStream) throws IOException {
    dataOutputStream.writeInt(attr1);
    dataOutputStream.writeInt(attr2);
    dataOutputStream.writeChar(attr3);
  }

  public void read(DataInputStream dataInputStream) throws IOException{
    attr1 = dataInputStream.readInt();
    attr2 = dataInputStream.readInt();
    attr3 = dataInputStream.readChar();
  }
}
