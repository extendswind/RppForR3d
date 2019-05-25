

# Basic

The whole raster data is divided into many cells using grid index, every cell is stored in a file and named by
grid_filename_rowId_colId.




# Some problem exist

- [ ] write machine not considered
- [ ] storage position (disks et. al.) are not considered, just choose the datanode


- [ ] every file must have the same size

- [ ] multi-file overlap is not tested

TODO GroupInfo类暂时设为3 3 1 1，后面考虑从数据库或者专门的索引文件里传入。

TODO 暂时简化整个模型的设计，不然RecordReader上三维之后太恶心了...
TODO 不考虑半径超过一个文件的情况 因此groupYSize为2  groupZSize在isFirstGroup为true时为2，false时为3

在InputFormat和BlockPlacementPolicy中


将之前针对2d代码改到3d上



# File Input

设置input路径为 spatialFilePath/info.dat

从info.dat中读取grid的信息，然后进行计算。


# SpatialInputFormatGroup

InputFormat 输入info.dat得到整个网格的结构信息，然后根据GroupInfo按分组将文件划分到InputSplit，最后用RecordReader解析。


# Warning

- 文件编号的顺序，在二维情况下，标号为（row, col），三维情况下为（x,y,z)，而x对应于col，y对应于row。

GroupInfo.zSize 暂时只考虑为1的情况

