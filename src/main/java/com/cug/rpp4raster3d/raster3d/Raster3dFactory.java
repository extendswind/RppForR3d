package com.cug.rpp4raster3d.raster3d;

import org.apache.hadoop.conf.Configuration;

import java.awt.image.Raster;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.util.Arrays;

public class Raster3dFactory {
  /**
   * get a xDim*yDim*zDim Raster3D object, the specific type of Raster3D is set in conf
   */
  public static Raster3D getRaster3D(Configuration conf, int xDim, int yDim, int zDim){
    String r3dClassName = conf.get("r3d.class.name", "com.cug.rpp4raster3d.raster3d.NormalRaster3D");
    Raster3D raster3D = null;
    try {
      Constructor raster3dConstructor = Class.forName(r3dClassName)
          .getConstructor(int.class, int.class, int.class);
      raster3D = (Raster3D) raster3dConstructor.newInstance(xDim, yDim, zDim);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return raster3D;
  }

  public static Raster3D getRaster3D(Configuration conf, Raster3D[] raster3Ds){
    String r3dClassName = conf.get("r3d.class.name", "com.cug.rpp4raster3d.raster3d.NormalRaster3D");
    Raster3D raster3D = null;
    try {
      Class r3dClass = Class.forName(r3dClassName);
      Class r3dArrayClass = Class.forName("[L" + r3dClassName + ";"); // 通过反射创建数组类的方式

      // 创建一个r3dClass类的数组，为了将Raster3D的数组类型cast到r3dClass类的数组，由于数组之间不能直接cast
      Object r3dArray = Array.newInstance(r3dClass, raster3Ds.length);
      for(int i=0; i<raster3Ds.length; i++){
        Array.set(r3dArray, i, r3dClass.cast(raster3Ds[i]));
      }

      // 获取构造函数并调用
      Constructor raster3dConstructor2 = r3dClass.getConstructor(r3dArrayClass);
      raster3D = (Raster3D) raster3dConstructor2.newInstance(r3dArray);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return raster3D;
  }
}
