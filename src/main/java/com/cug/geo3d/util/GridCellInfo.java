package com.cug.geo3d.util;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefaultSpatial;

import java.util.Objects;


/**
 * id and file of every cell in the grid
 * mainly used in BlockPlacementPolicyDefaultSpatial for *getting cell info from filename*
*/
public class GridCellInfo {
  public int rowId;
  public int colId;
  public String filename; // 不带grid编号的文件名
  public String filepath;

  public GridCellInfo() {
  }

  public GridCellInfo(int rowId, int colId) {
    this.rowId = rowId;
    this.colId = colId;
  }


  public GridCellInfo(GridCellInfo info) {
    this.rowId = info.rowId;
    this.colId = info.colId;
    this.filename = info.filename;
    this.filepath = info.filepath;
  }

  /**
   * 通过文件名判断是否为grid index
   * filename example:  grid_filename_0_1
   *
   * @param srcFile 带路径的文件名
   * @return grid index时返回对象 否则null
   */
  public static GridCellInfo getGridCellInfoFromFilename(String srcFile) {
    String[] filenameSplit = FilenameUtils.getName(srcFile).split("_");
    if (filenameSplit.length != 4 || !filenameSplit[0].equals(SpatialConstant.GRID_INDEX_PREFIX)) {
      return null;
    }
    GridCellInfo pos = new GridCellInfo();
    pos.rowId = Integer.parseInt(filenameSplit[2]);
    pos.colId = Integer.parseInt(filenameSplit[3]);
    pos.filename = SpatialConstant.GRID_INDEX_PREFIX + "_" + filenameSplit[1];
    pos.filepath = FilenameUtils.getPath(srcFile);
    return pos;
  }


}
