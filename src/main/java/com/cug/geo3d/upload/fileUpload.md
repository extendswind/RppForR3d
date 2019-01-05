

# Upload Spatial Raster Data to HDFS


新建文件夹与文件名相同

对文件划分成rowSize*colSize块，每个块保存到文件grid_filename_rowId_colId。

新建文件info_rowSize_colSize_cellRowSize_cellColSize用于保存文件的基本空间信息

将所有划分后的文件与info文件上传到HDFS。SpatialBlockPlacementPolicy会对grid开头的划分后数据文件使用空间数据的存储机制，对info文件使用默认存储机制。

# TODO

当前假设上传的文件每个文件块大小相同，**不存在**无法整除一类的情况  如100×100的像素被划分为3×3。也**没有**让最后一行或列的数据较小，如固定每个块的大小为30×30,将100×100像素划分后最后一行成为10×30。

WARN: 文件名中不能含_
WARN: file should be uploaded in row order
