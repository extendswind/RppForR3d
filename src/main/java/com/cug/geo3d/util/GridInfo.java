package com.cug.geo3d.util;

/**
 * not used now
 */
public class GridInfo {
  public long rowId;
  public long colId;
  public String filename; // 不带grid编号的文件名
  public String filepath;

  GridInfo() {
  }

  GridInfo(GridInfo info) {
    this.rowId = info.rowId;
    this.colId = info.colId;
    this.filename = info.filename;
    this.filepath = info.filepath;
  }
}
