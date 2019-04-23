package com.cug.geo3d.util;


/**
 * group info for the grid
 */
public class GroupInfo {
    public int rowSize; // put *rowSize* row files to a group
    public int colSize;
    public int rowOverlap; // overlap *rowOverlap* rows between two row group
    public int colOverlap;

    public GroupInfo(int rowSize, int colSize, int rowOverlap, int colOverlap){
        this.rowSize = rowSize;
        this.colSize = colSize;
        this.rowOverlap = rowOverlap;
        this.colOverlap = colOverlap;
    }
}
