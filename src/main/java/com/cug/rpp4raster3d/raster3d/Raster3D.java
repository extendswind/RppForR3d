package com.cug.rpp4raster3d.raster3d;


import java.io.DataInput;
import java.io.IOException;

/**
 * 注意体元数量限制在一个int的数值范围内
 * 由于java对象需要占用额外的十几个字节，因此使用对象作为体元会浪费大量的空间
 *
 * TODO 在Raster3dFactory中，调用了子类的构造函数  public NormalRaster3D(NormalRaster3D[]  raster3ds)
 * TODO 所有子类最好实现此构造函数避免出错
 * TODO 后期把这个构造转为一个创造类的接口函数更为合理
 *
 * TODO 作为value值的序列化
 */
public abstract class Raster3D {

//  public abstract Raster3D(int xDim, int yDim, int zDim);


  public abstract int getXDim();
  public abstract int getYDim();
  public abstract int getZDim();

  public abstract byte[] getAttr1();
  public abstract int getCellSize();

  /**
   * read the cell value from dataInput
   */
  public abstract void readAttr(int index, DataInput dataInput) throws IOException;


  public abstract void setAttr(int index, VoxelAttrsBase cellattrs);

  // return a subclass of VoxelAttrsBase for the storage of all attributes
  public abstract VoxelAttrsBase getAttr(int index);




  // ----- operation -----

  // used for simple sampling
  public abstract Raster3D averageSampling(int radius);

  public abstract Raster3D getZRegion(int zStart, int zEnd);

  // 把z方向上比startLayerZ大的部分，复制到从z为0的位置的部分。
  // 用在SpatialRecordReaderSimpleRaster3D中，
  public abstract void upMoveLayerData(int startLayerZ);







  //  public void writeAttr(int index, DataOutput dataOutput) throws IOException {
//    dataOutput.writeByte(attr[index]);
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
//      dataOutput.writeByte(attr[i]);
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
//      attr[i] = in.readByte();
//    }
//  }



}
