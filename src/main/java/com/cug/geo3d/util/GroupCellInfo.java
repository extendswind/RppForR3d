package com.cug.geo3d.util;

/**
 * for every grid cell
 */
public class GroupCellInfo extends Coord {
//    public int rowId;
//    public int colId;

  public Coord OCCoord; // right overlapped column
  public Coord ORCoord; // bottom overlapped row

//    boolean isROC; // right overlapped colum
//    boolean isBOR; // bottom overlapped row

//    String mainReplicaPos;
//    String OCCoord; // right overlapped column
//    String ORCoord; // bottom overlapped row

  public GroupCellInfo() {

  }

  public GroupCellInfo(int rowId, int colId, Coord OCCoord, Coord ORCoord) {
    this.rowId = rowId;
    this.colId = colId;
    if (OCCoord == null) {
      this.OCCoord = null;
      this.ORCoord = null;
    } else {
      this.OCCoord = new Coord(OCCoord);
      this.ORCoord = new Coord(ORCoord);
    }
  }

  public GroupCellInfo(GroupCellInfo info) {
    this(info.rowId, info.colId, info.OCCoord, info.ORCoord);
  }

  /**
   * 通过GridCellInfo (rowId colId) 和 GroupInfo，计算GridCell所在GroupCell的GroupCellInfo
   */
  public static GroupCellInfo getFromGridCellInfo(GridCellInfo gridCellInfo, GroupInfo groupInfo) {
    GroupCellInfo groupCellInfo = new GroupCellInfo();

    // calculate rowId and colId of groupCellInfo
    if (gridCellInfo.colId < groupInfo.colSize) {
      groupCellInfo.colId = 0;
    } else {
      // overlapped columns 算在左边的group里
      groupCellInfo.colId = 1 + (gridCellInfo.colId - groupInfo.colSize) / (groupInfo.colSize - groupInfo.colOverlapSize);
    }
    groupCellInfo.rowId = gridCellInfo.rowId / groupInfo.rowSize;

    // Judging whether or not it is an overlapped column
    if (gridCellInfo.colId != 0 &&
        gridCellInfo.colId % (groupInfo.colSize - groupInfo.colOverlapSize) < groupInfo.colOverlapSize) {
      groupCellInfo.OCCoord = new Coord(groupCellInfo.rowId, groupCellInfo.colId);
    }

    // Judging whether or not it is an overlapped row
    if (gridCellInfo.rowId % (groupInfo.rowSize) < groupInfo.rowOverlapSize && (groupCellInfo.rowId != 0)) {
      groupCellInfo.ORCoord = new Coord(groupCellInfo.rowId - 1, 0);
    }
    if (gridCellInfo.rowId % (groupInfo.rowSize) >= groupInfo.rowSize - groupInfo.rowOverlapSize) {
      groupCellInfo.ORCoord = new Coord(groupCellInfo.rowId, 0);
    }

    return groupCellInfo;
  }
}
