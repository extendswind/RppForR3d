
因为Hadoop有些类设计为包内才能使用，为了避免更多的拉入hadoop源码，还是使用一样的包名了...

BlockPlacementPolicyDefaultSpatial 与 BlockPlacmentPolicySpatialUtil 为先前使用的代码，已弃用。

SpatialInfoTableOperator and its sub class is used for save and read the spatial information table
, SpatialInfoTableOperatorForFile store the table in file.
