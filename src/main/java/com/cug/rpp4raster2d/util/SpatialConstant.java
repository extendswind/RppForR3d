package com.cug.rpp4raster2d.util;

// 定义一些常量 暂时放在SpatialInputFormat
public class SpatialConstant {
    public static final String GRID_INDEX_PREFIX = "grid";  // 使用grid索引的文件，文件名会以此变量开头
    public static final String RASTER_3D_INDEX_PREFIX = "r3d";  // 使用raster 3d索引的文件，文件名会以此变量开头
    public static final int ROW_OVERLAPPED_GROUP_SPLIT_ID_BEGIN = 100000;  // 使用grid索引的文件，文件名会以此变量开头
    public static final String READING_STATISTICS_FILE = "/home/fly/read_statistics_rpp.txt";  // 读统计文件位置

    // for configuration of hadoop
    public static final String RASTER_3D_CLASS_NAME_KEY = "r3d.class.name";  // 读统计文件位置
}
