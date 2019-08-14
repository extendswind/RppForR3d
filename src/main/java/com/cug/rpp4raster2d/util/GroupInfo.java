package com.cug.rpp4raster2d.util;


/**
 * group info for the grid, designed for 2D and add *zSize* for 3D
 */
public class GroupInfo {
    public int rowSize; // put *rowSize* row files to a group
    public int colSize;
    public int rowOverlapSize; // overlap *rowOverlapSize* rows between two row group  暂时只考虑为1的情况
    public int colOverlapSize;

    public int zSize = 2; // TODO only 1 z layer is considered

    public GroupInfo(GroupInfo info){
        rowSize = info.rowSize;
        colSize = info.colSize;
        rowOverlapSize = info.rowOverlapSize;
        colOverlapSize = info.colOverlapSize;
    }

    public GroupInfo(int rowSize, int colSize, int rowOverlapSize, int colOverlapSize){
        this.rowSize = rowSize;
        this.colSize = colSize;
        this.rowOverlapSize = rowOverlapSize;
        this.colOverlapSize = colOverlapSize;
    }

    public static GroupInfo getDefaultGroupInfo(){
      return new GroupInfo(2, 2, 1,1);
    }

}
