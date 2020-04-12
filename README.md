
This repo contains the code related to the paper *An efficient group-based replica placement policy for large-scale geospatial 3D raster data on Hadoop*.
 
The replica placement policy for 3D raster data is realized in class *BlockPlacementPolicyRasterGroupRefactor*.

To process the 3D raster data that are uploaded through this policy, class *SpatialFileInputFormatGroupRaster3D* can be used in  Apache Spark or Hadoop MapReduce. Class *SpatialFileInputFormatSimpleRaster3D* is available for comparison in default replica placement policy. 