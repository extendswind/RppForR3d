package com.cug.rpp4raster3d.spatialInputFormat;

import com.cug.rpp4raster3d.raster3d.SimpleCellAttrs;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * TODO only used in simple raster 3d now
 * 用于SpatialInputFormat得到的value类型   TODO 最好改个带value的名字
 * 由{@link SpatialRecordReaderGroupRaster3D} 生成
 *
 * 记录每一个split的宽、高、数据
 *
 */
public class InputSplitWritableRaster3D implements Writable{

  IntWritable xDim;
  IntWritable yDim;
  IntWritable zDim;
  SimpleCellAttrs[] data;

  public InputSplitWritableRaster3D(){

  }

  public InputSplitWritableRaster3D(IntWritable xDim, IntWritable yDim, IntWritable zDim, SimpleCellAttrs[] data){
    setAttributes(xDim, yDim, zDim, data);
  }

  public void setAttributes(IntWritable xDim, IntWritable yDim, IntWritable zDim, SimpleCellAttrs[] data){
    this.xDim = xDim;
    this.yDim = yDim;
    this.zDim = zDim;
    this.data = data;
  }

  public IntWritable getXDim(){return xDim; }
  public IntWritable getYDim(){return yDim; }
  public IntWritable getZDim(){return zDim;}
  public SimpleCellAttrs[] getData() { return data; }

  @Override
  public void readFields(DataInput in) throws IOException {
    xDim = new IntWritable(in.readInt());
    yDim = new IntWritable(in.readInt());
    zDim = new IntWritable(in.readInt());

    data = new SimpleCellAttrs[xDim.get() * yDim.get() * zDim.get()];// construct data
    for (int i = 0; i < data.length; i++) {
      data[i] = new SimpleCellAttrs();
      data[i].read(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    xDim.write(out);
    yDim.write(out);
    zDim.write(out);
    for (int i = 0; i < data.length; i++) {
      data[i].write(out);
    }
  }

}
