package com.cug.rpp4raster3d.raster3d;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 由于java不能继承静态函数，而且后面用得比较多，所以基类有点多余
public abstract class CellAttrsBase{
  public abstract void write(DataOutput dataOutput) throws IOException;
  public abstract void read(DataInput dataInput) throws IOException;
  public abstract int getSize();
}
