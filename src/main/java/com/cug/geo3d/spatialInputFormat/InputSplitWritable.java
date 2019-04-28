package com.cug.geo3d.spatialInputFormat;

import com.cug.geo3d.spatialInputFormat.backup.SpatialRecordReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 用于SpatialInputFormat得到的value类型
 * 由{@link SpatialRecordReader} 生成
 *
 * 记录每一个split的宽、高、数据
 *
 */
public class InputSplitWritable implements Writable{

  private IntWritable width;
  private IntWritable height;
  private IntWritable[] data;

  public InputSplitWritable(){

  }

  public InputSplitWritable(IntWritable width, IntWritable height, IntWritable[] data){
    setAttributes(width, height, data);
  }

  public void setAttributes(IntWritable width, IntWritable height, IntWritable[] data) {
    this.width = width;
    this.height = height;
    this.data = data;
  }

  public IntWritable getWidth() { return width; }
  public IntWritable getHeight() { return height; }
  public IntWritable[] getData() { return data; }

  @Override
  public void readFields(DataInput in) throws IOException {
    data = new IntWritable[in.readInt()];          // construct data
    for (int i = 0; i < data.length; i++) {
      data[i] = new IntWritable();
      data[i].readFields(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    width.write(out);
    height.write(out);
    for (int i = 0; i < data.length; i++) {
      data[i].write(out);
    }
  }

}
