package com.cug.geo3d.util;


/**
 * group info for the grid
 */
public class GroupInfo {
    public int rowSize; // put *rowSize* row files to a group
    public int colSize;
    public int rowOverlapSize; // overlap *rowOverlapSize* rows between two row group
    public int colOverlapSize;

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
      return new GroupInfo(3, 3, 1,1);
    }

}
