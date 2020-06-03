
This repo contains the code related to the paper *An efficient group-based replica placement policy for large-scale geospatial 3D raster data on Hadoop*.
 
The replica placement policy for 3D raster data is realized in class *BlockPlacementPolicyRasterGroupRefactor*.

To process the 3D raster data uploaded through the group-based policy, class *SpatialFileInputFormatGroupRaster3D* is implemented for generating appropriate key-value pairs on Apache Spark or Hadoop MapReduce. 

Class *SpatialFileInputFormatSimpleRaster3D* is available for the comparison of 3D raster data uploaded through the default replica placement policy. 
