

# Basic

The whole raster data is divided into many cells using grid index, every cell is stored in a file and named by
grid_filename_rowId_colId.




# Some problem exist

- [ ] write machine not considered
- [ ] storage position (disks et. al.) are not considered, just choose the datanode


- [ ] every file must have the same size

- [ ] multi-file overlap is not tested


TODO GroupInfo类暂时设为3 3 1 1，后面考虑从数据库或者专门的索引文件里传入。

在InputFormat和BlockPlacementPolicy中


将之前针对2d代码改到3d上



# File Input

设置input路径为 spatialFilePath/info.dat

从info.dat中读取grid的信息，然后进行计算。


# SpatialInputFormatGroup

InputFormat 输入info.dat得到整个网格的结构信息，然后根据GroupInfo按分组将文件划分到InputSplit，最后用RecordReader解析。


