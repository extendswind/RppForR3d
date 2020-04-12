package com.cug.rpp4raster3d.util;

public class Coord {
    public int yId;
    public int xId;
    public int zId;
    public Coord(){

    }
    public Coord(Coord coord){
        yId = coord.yId;
        xId = coord.xId;
        zId = coord.zId;
    }
    public Coord(int xId, int yId, int zId){
        this.yId = yId;
        this.xId = xId;
        this.zId = zId;
    }
}
