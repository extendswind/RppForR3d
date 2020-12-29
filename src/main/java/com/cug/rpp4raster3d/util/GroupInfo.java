package com.cug.rpp4raster3d.util;


/**
 * group info for the grid, designed for 2D and add *zSize* for 3D
 */
public class GroupInfo {
    public int rowSize; // put *rowSize* row files to a group
    public int colSize;
    public int rowOverlapSize; // overlap *rowOverlapSize* rows between two row group  TODO not used
    public int colOverlapSize; // TODO not used
    public int zSize;

    public GroupInfo(GroupInfo info){
        rowSize = info.rowSize;
        colSize = info.colSize;
        rowOverlapSize = info.rowOverlapSize;
        colOverlapSize = info.colOverlapSize;
        zSize = info.zSize;
    }

    /** WARNG!!!  此函数赋值顺序为row col， *not* X Y
     *
     * @param rowSize
     * @param colSize
     * @param rowOverlapSize
     * @param colOverlapSize
     */
    public GroupInfo(int rowSize, int colSize, int rowOverlapSize, int colOverlapSize){
        this.rowSize = rowSize;
        this.colSize = colSize;
        this.rowOverlapSize = rowOverlapSize;
        this.colOverlapSize = colOverlapSize;
        zSize = 2;
    }

    public static GroupInfo getDefaultGroupInfo(){
        return new GroupInfo(3, 2, 1,1);
    }
}
