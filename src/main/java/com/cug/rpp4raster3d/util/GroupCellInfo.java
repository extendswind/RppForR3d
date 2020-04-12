package com.cug.rpp4raster3d.util;

/**
 * for every grid cell
 */
public class GroupCellInfo extends Coord {

  public Coord OCCoord; // right overlapped column
  public Coord ORCoord; // bottom overlapped row

  public GroupCellInfo() {

  }

  public GroupCellInfo(int rowId, int colId, int zId, Coord OCCoord, Coord ORCoord) {
    super(colId, rowId, zId);
    if (OCCoord == null) {
      this.OCCoord = null;
      this.ORCoord = null;
    } else {
      this.OCCoord = new Coord(OCCoord);
      this.ORCoord = new Coord(ORCoord);
    }
  }

  public GroupCellInfo(GroupCellInfo info) {
    this(info.yId, info.xId, info.zId, info.OCCoord, info.ORCoord);
  }

  /**
   * 通过GridCellInfo (rowId colId) 和 GroupInfo，计算GridCell所在GroupCell的GroupCellInfo
   */
  public static GroupCellInfo getFromGridCellInfo(CellIndexInfo cellIndexInfo, GroupInfo groupInfo, int xSplitSize,
                                                  int ySplitSize) {
    GroupCellInfo groupCellInfo = new GroupCellInfo();

    // calculate rowId and colId of groupCellInfo
    if (cellIndexInfo.colId < groupInfo.colSize) {
      groupCellInfo.xId = 0;
    } else {
      // overlapped columns 算在左边的group里
      groupCellInfo.xId =
          1 + (cellIndexInfo.colId - groupInfo.colSize) / (groupInfo.colSize - groupInfo.colOverlapSize);
    }
    groupCellInfo.yId = cellIndexInfo.rowId / groupInfo.rowSize;
    groupCellInfo.zId = cellIndexInfo.zId / groupInfo.zSize;

    // Judging whether or not it is an overlapped column
    // right most column is not be seen as overlapped column
    if (cellIndexInfo.colId != 0 &&
        cellIndexInfo.colId % (groupInfo.colSize - groupInfo.colOverlapSize) < groupInfo.colOverlapSize &&
        cellIndexInfo.colId != xSplitSize - 1) {
      groupCellInfo.OCCoord = new Coord(groupCellInfo.xId + 1, groupCellInfo.yId, groupCellInfo.zId);
    }

    // Judging whether or not it is an overlapped row
    if (cellIndexInfo.rowId % (groupInfo.rowSize) < groupInfo.rowOverlapSize && (groupCellInfo.yId != 0)) {
      groupCellInfo.ORCoord = new Coord(0, groupCellInfo.yId - 1, groupCellInfo.zId);
    }
    if (cellIndexInfo.rowId % (groupInfo.rowSize) >= groupInfo.rowSize - groupInfo.rowOverlapSize &&
        cellIndexInfo.rowId != ySplitSize - 1) {
      groupCellInfo.ORCoord = new Coord(0, groupCellInfo.yId, groupCellInfo.zId);
    }

    return groupCellInfo;
  }
}
