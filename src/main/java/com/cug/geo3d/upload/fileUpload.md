

# Upload Spatial Raster Data to HDFS


新建文件夹与文件名相同

对文件划分成rowSize*colSize块，每个块保存到文件grid_filename_rowId_colId。

新建文件info_rowSize_colSize用于保存文件的基本空间信息

将所有划分后的文件与info文件上传到HDFS。SpatialBlockPlacementPolicy会对grid开头的划分后数据文件使用空间数据的存储机制，对info文件使用默认存储机制。

