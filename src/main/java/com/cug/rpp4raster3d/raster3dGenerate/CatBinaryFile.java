package com.cug.rpp4raster3d.raster3dGenerate;

import java.io.*;

public class CatBinaryFile{
       public static void main(String[] argvs) throws IOException{
               DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream("grid_test.dat_0_2")));
               System.out.println(inputStream.readInt());
       }
}
