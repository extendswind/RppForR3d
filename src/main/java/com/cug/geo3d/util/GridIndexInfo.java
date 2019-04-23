package com.cug.geo3d.util;



public class GridIndexInfo {
  public int cellRowNum;
  public int cellColNum;
  public int cellRowSize;
  public int cellColSize;

  public GridIndexInfo(int cellRowNum, int cellColNum, int cellRowSize, int cellColSize){
    this.cellColNum = cellColNum;
    this.cellRowNum = cellRowNum;
    this.cellColSize = cellColSize;
    this.cellRowSize = cellRowSize;
  }
}
