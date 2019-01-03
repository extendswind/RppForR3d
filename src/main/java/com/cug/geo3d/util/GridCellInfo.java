package com.cug.geo3d.util;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefaultSpatial;

import java.util.Objects;


/**
 * mainly used in BlockPlacementPolicyDefaultSpatial for *get cell info from filename*
*/
public class GridCellInfo {
  public int rowId;
  public int colId;
  public String filename; // 不带grid编号的文件名
  public String filepath;

  public GridCellInfo() {
  }

  public GridCellInfo(GridCellInfo info) {
    this.rowId = info.rowId;
    this.colId = info.colId;
    this.filename = info.filename;
    this.filepath = info.filepath;
  }

  /**
   * 通过文件名判断是否为grid index
   *
   * @param srcFile 带路径的文件名
   * @param pos     如果是，则通过pos传出位置
   * @return grid index时返回ture 否则false
   */
  public static boolean getGridIndexFromFilename(String srcFile, GridCellInfo pos) {
    String[] filenameSplit = FilenameUtils.getName(srcFile).split("_");
    if (filenameSplit.length != 4 || !filenameSplit[0].equals(BlockPlacementPolicyDefaultSpatial.GRID_INDEX_PREFIX)) {
      return false;
    }

    if (pos == null)
      pos = new GridCellInfo();
    pos.rowId = Integer.parseInt(filenameSplit[2]);
    pos.colId = Integer.parseInt(filenameSplit[3]);
    pos.filename = BlockPlacementPolicyDefaultSpatial.GRID_INDEX_PREFIX + "_" + filenameSplit[1];
    pos.filepath = FilenameUtils.getPath(srcFile);
    return true;
  }

}
