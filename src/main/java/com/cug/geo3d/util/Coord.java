package com.cug.geo3d.util;

public class Coord {
    public int rowId;
    public int colId;
    public Coord(){

    }
    public Coord(Coord coord){
        rowId = coord.rowId;
        colId = coord.colId;
    }
    public Coord(int rowId, int colId){
        this.rowId = rowId;
        this.colId = colId;
    }
}
