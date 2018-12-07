package com.cug.geo3d.util;

public class GridIndexInfo {
  public int gridRowSize;
  public int gridColSize;
  public int cellRowSize;
  public int cellColSize;

  public GridIndexInfo(int gridRowSize, int gridColSize, int cellRowSize, int cellColSize){
    this.gridColSize = gridColSize;
    this.gridRowSize = gridRowSize;
    this.cellColSize = cellColSize;
    this.cellRowSize = cellRowSize;
  }
}
