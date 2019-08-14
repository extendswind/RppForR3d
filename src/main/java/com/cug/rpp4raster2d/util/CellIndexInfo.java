package com.cug.rpp4raster2d.util;

import org.apache.commons.io.FilenameUtils;


/**
 * id and file of every cell in the grid
 * mainly used in BlockPlacementPolicyDefaultSpatial for *getting cell info from filename*
 * designed for 2D and add zId for 3D
 */
public class CellIndexInfo {
  public int rowId;
  public int colId;
  public int zId;
  public String filename; // 不带grid编号的文件名
  public String filepath;

  public CellIndexInfo() {
  }

  public CellIndexInfo(int rowId, int colId) {
    this.rowId = rowId;
    this.colId = colId;
  }


  public CellIndexInfo(CellIndexInfo info) {
    this.rowId = info.rowId;
    this.colId = info.colId;
    this.filename = info.filename;
    this.filepath = info.filepath;
    this.zId = info.zId;
  }


  /**
   * 通过文件名判断是否为grid index 或者 raster3d
   * filename example: (grid) grid_filename_0_1   (raster 3d) raster3d_0_1_1
   *
   * @param srcFile 带路径的文件名
   * @return grid index时返回对象 否则null
   */
  public static CellIndexInfo getGridCellInfoFromFilename(String srcFile) {
    String[] filenameSplit = FilenameUtils.getName(srcFile).split("_");
    CellIndexInfo pos = new CellIndexInfo();
    if (filenameSplit.length == 4 && filenameSplit[0].equals(SpatialConstant.GRID_INDEX_PREFIX)){
      pos.rowId = Integer.parseInt(filenameSplit[2]);
      pos.colId = Integer.parseInt(filenameSplit[3]);
      pos.filename = filenameSplit[0] + "_" + filenameSplit[1];
      pos.filepath = FilenameUtils.getPath(srcFile);
      pos.zId = 0;
      return pos;
    }
    else if(filenameSplit.length == 5 && filenameSplit[0].equals(SpatialConstant.RASTER_3D_INDEX_PREFIX)) {
      pos.rowId = Integer.parseInt(filenameSplit[3]);
      pos.colId = Integer.parseInt(filenameSplit[2]);
      pos.filename = filenameSplit[0] + "_" + filenameSplit[1];
      pos.filepath = FilenameUtils.getPath(srcFile);
      pos.zId = Integer.parseInt(filenameSplit[4]);
      return pos;
    } else {
      return null;
    }
  }

}
