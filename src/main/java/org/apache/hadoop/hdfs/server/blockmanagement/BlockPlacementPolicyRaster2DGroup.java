/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import com.cug.rpp4raster2d.util.CellIndexInfo;
import com.cug.rpp4raster2d.util.Coord;
import com.cug.rpp4raster2d.util.GroupCellInfo;
import com.cug.rpp4raster2d.util.GroupInfo;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import java.io.*;
import java.util.*;

import static org.apache.hadoop.util.Time.monotonicNow;


/**
 * grid index file
 * <p>
 * block placement of previous upload spatial file will be used in current block placement, so using a file to store the
 * information to avoiding the visit of namenode.
 * <p>
 * gridRowId gridColId datanode1 datanode2 ....
 * <p>
 * gridRowId and gridColId is -1 represent the split id which will be got in InputFormat, used for datanode balance.
 */


/**
 * The class is responsible for choosing the desired targets for placing block replicas.
 * <p>
 * 3D 数据会被视为多层 2D 数据处理
 */
@InterfaceAudience.Private
public class BlockPlacementPolicyRaster2DGroup extends BlockPlacementPolicy {

  private static final String enableDebugLogging =
      "For more information, please enable DEBUG log level on "
          + BlockPlacementPolicy.class.getName();

  private static final ThreadLocal<StringBuilder> debugLoggingBuilder
      = new ThreadLocal<StringBuilder>() {
    @Override
    protected StringBuilder initialValue() {
      return new StringBuilder();
    }
  };

  protected boolean considerLoad;
  private boolean preferLocalNode;
  protected NetworkTopology clusterMap;
  protected Host2NodesMap host2datanodeMap;
  private FSClusterStats stats;
  protected long heartbeatInterval;   // interval for DataNode heartbeats
  private long staleInterval;   // interval used to identify stale DataNodes
  //  private Configuration conf;


  /////////////////////////////////////////////
  // Spatial //

  // public static final String GRID_INDEX_PREFIX = "grid";  //　开头为此字符串的文件名为grid index
  private boolean isBalanceUpload;  // considering data balancing among datanode instead of random block placement

  // 假设上传文件的位置 TODO 考虑读取NFS文件或者HBase存储对应关系 上传空间文件时记录先前上传文件的位置
  private final String GROUP_INFO_FILE_PATH = "/tmp/";

  GroupInfo groupInfo;


  //  private final String SPATIAL_STORAGE_PATH = "/user/sparkl/spatial_data";
  //////////////////////////////////////////

  /**
   * A miss of that many heartbeats is tolerated for replica deletion policy.
   */
  protected int tolerateHeartbeatMultiplier;

  protected BlockPlacementPolicyRaster2DGroup() {
    isBalanceUpload = false;
    groupInfo = GroupInfo.getDefaultGroupInfo(); // TODO 从哪获取合适的信息，上传文件和配置？
  }

  @Override
  public void initialize(Configuration conf, FSClusterStats stats,
                         NetworkTopology clusterMap,
                         Host2NodesMap host2datanodeMap) {
    this.considerLoad = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY, true);
    this.stats = stats;
    this.clusterMap = clusterMap;
    this.host2datanodeMap = host2datanodeMap;
    this.heartbeatInterval = conf.getLong(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 1000;
    this.tolerateHeartbeatMultiplier = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_KEY,
        DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_DEFAULT);
    this.staleInterval = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);
    this.preferLocalNode = conf.getBoolean(
        DFSConfigKeys.
            DFS_NAMENODE_BLOCKPLACEMENTPOLICY_DEFAULT_PREFER_LOCAL_NODE_KEY,
        DFSConfigKeys.
            DFS_NAMENODE_BLOCKPLACEMENTPOLICY_DEFAULT_PREFER_LOCAL_NODE_DEFAULT);
    //    this.conf = conf;
  }


  // 判断是否为空间数据，进入对应的函数处理
  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath,
                                            int numOfReplicas,
                                            Node writer,
                                            List<DatanodeStorageInfo> chosenNodes,
                                            boolean returnChosenNodes,
                                            Set<Node> excludedNodes,
                                            long blocksize,
                                            final BlockStoragePolicy storagePolicy) {
    String filename = FilenameUtils.getName(srcPath);
    // 通过文件名判断是否为空间索引，并取索引中的位置
    CellIndexInfo cellIndexInfo = CellIndexInfo.getGridCellInfoFromFilename(filename);
    if (cellIndexInfo != null) { // 对于网格索引的文件
      DatanodeStorageInfo[] result = chooseTargetSpatial(cellIndexInfo, numOfReplicas, writer, chosenNodes,
          returnChosenNodes,
          excludedNodes, blocksize, storagePolicy);

      StringBuilder resultStr = new StringBuilder();
      for (DatanodeStorageInfo info : result) {
        resultStr.append(info.getDatanodeDescriptor().toString());
        resultStr.append(" ");
      }
      LOG.debug("chooseTarget result: " + resultStr.toString());

      return result;

    } else {  // 对于普通文件
      return chooseTarget(numOfReplicas, writer, chosenNodes, returnChosenNodes,
          excludedNodes, blocksize, storagePolicy);
    }
  }


  public DatanodeStorageInfo[] chooseTargetSpatial(CellIndexInfo cellIndexInfo,
                                                   int numOfReplicas,
                                                   Node writer,
                                                   List<DatanodeStorageInfo> chosenNodes,
                                                   boolean returnChosenNodes,
                                                   Set<Node> excludedNodes,
                                                   long blocksize,
                                                   final BlockStoragePolicy storagePolicy) {

    // final result of the algorithm
    final List<DatanodeStorageInfo> results = new ArrayList<>();

    boolean avoidStaleNodes = false;  // TODO  for debug
    //(stats != null && stats.isAvoidingStaleDataNodesForWrite());

    // 根据集群结构调整副本的数量，输入已经选择的副本数和还要选择的副本数，返回调整后的副本数与MaxNodesPerRack
    // 不知道具体作用？
    int[] funResults = getMaxNodesPerRack(chosenNodes.size(), numOfReplicas);
    numOfReplicas = funResults[0];
    int maxNodesPerRack = funResults[1];

    // 一些变量的初始化
    // choose storage types; use fallbacks for unavailable storages
    final List<StorageType> requiredStorageTypes = storagePolicy
        .chooseStorageTypes((short) numOfReplicas,
            DatanodeStorageInfo.toStorageTypes(results),
            EnumSet.noneOf(StorageType.class), true);
    final EnumMap<StorageType, Integer> storageTypes =
        getRequiredStorageTypes(requiredStorageTypes);

    // prefer not using other replica position
    //Set<Node> softExcludedNodes = new TreeSet<Node>();
    Set<Node> softExcludedNodes = new LinkedHashSet<>();

    if (excludedNodes == null) {
      excludedNodes = new LinkedHashSet<>();
    }

    for (DatanodeStorageInfo storage : chosenNodes) {
      // add localMachine and related nodes to excludedNodes
      addToExcludedNodes(storage.getDatanodeDescriptor(), excludedNodes);
    }


    String groupInfoFile = GROUP_INFO_FILE_PATH + "/GROUP_" + cellIndexInfo.filename;
    if (cellIndexInfo.rowId == 0 && cellIndexInfo.colId == 0 && (cellIndexInfo.zId % groupInfo.zSize == 0)) {
      new File(groupInfoFile).delete();
    }

    GroupCellInfo groupCellInfo = GroupCellInfo.getFromGridCellInfo(cellIndexInfo, groupInfo);

    //    if(info.rowId == 0)

    // left-top grid node will get a random place, the same to default block placement
    //    if (cellIndexInfo.rowId == 0 && cellIndexInfo.colId == 0) {
    //      DatanodeStorageInfo[] resultNodes = chooseTarget(numOfReplicas, writer, chosenNodes, returnChosenNodes,
    //          excludedNodes, blocksize, storagePolicy);
    //      if (resultNodes != null) {
    //        saveGroupReplicaPosToFile(resultNodes[0].getDatanodeDescriptor(), groupCellInfo, MG_PREFIX,
    //        groupInfoFile);
    //      }
    //      return resultNodes;
    //    }


    // ----------  core algorithm for spatial optimization  ----------------
    // 首先给overlapped group分配位置
    if (cellIndexInfo.rowId % groupInfo.rowSize == 0 && cellIndexInfo.colId == 0) {
      // choose a random node for overlapped row group (0,0)
      List<DatanodeStorageInfo> ORNodeResult = new ArrayList<>();
      DatanodeStorageInfo ORNode = chooseRandomSpatial(NodeBase.ROOT, excludedNodes, blocksize, maxNodesPerRack,
          ORNodeResult, avoidStaleNodes, storageTypes, softExcludedNodes);
      saveGroupReplicaPosToFile(ORNode.getDatanodeDescriptor(), groupCellInfo, OR_PREFIX, groupInfoFile);
      storageTypes.put(ORNode.getStorageType(), storageTypes.get(ORNode.getStorageType()) + 1);
      excludedNodes.clear();
      softExcludedNodes.clear();
    }

    // 读取当前group所在的位置放置第一个副本
    String currentGroupReplicaPos = readGroupReplicaPosFromFile(groupInfoFile, groupCellInfo, MG_PREFIX);
    if (currentGroupReplicaPos != null) {
      DatanodeDescriptor mainGroupNode = (DatanodeDescriptor) clusterMap.getNode(currentGroupReplicaPos);
      DatanodeStorageInfo mainGroupStorageInfo = getDatanodeStorageInfo(mainGroupNode);
      results.add(mainGroupStorageInfo);
      excludedNodes.add(mainGroupNode);
      storageTypes
          .put(mainGroupStorageInfo.getStorageType(), storageTypes.get(mainGroupStorageInfo.getStorageType()) + 1);
    } else {  // 当前group的第一个副本
      // 将周围Overlapped Row所在节点加入到excludedNodes
      findAndAddToExcludedNode(groupInfoFile, new Coord(groupCellInfo.rowId - 1, 0), OR_PREFIX, excludedNodes);
      findAndAddToExcludedNode(groupInfoFile, new Coord(groupCellInfo.rowId, 0), OR_PREFIX, excludedNodes);

      chooseTargetSpatial(1, excludedNodes, softExcludedNodes, results,
          avoidStaleNodes, maxNodesPerRack, storageTypes, blocksize, cellIndexInfo.filename);
      saveGroupReplicaPosToFile(results.get(0).getDatanodeDescriptor(), groupCellInfo, MG_PREFIX, groupInfoFile);
    }




    // overlapped column
    // 假设overlapped column size只为1
    if (groupCellInfo.OCCoord != null) {
      GroupCellInfo rightGroupCellInfo = new GroupCellInfo(groupCellInfo.rowId, groupCellInfo.colId + 1,
          null, null);
      String rightGroupReplicaPos = readGroupReplicaPosFromFile(groupInfoFile, rightGroupCellInfo, MG_PREFIX);

      if (rightGroupReplicaPos == null) { // overlapped column处的第一个文件（当前组右上角）  负责写入右边group所在的位置
        findAndAddToExcludedNode(groupInfoFile, new Coord(groupCellInfo.rowId - 1, 0), OR_PREFIX, excludedNodes);
        findAndAddToExcludedNode(groupInfoFile, new Coord(groupCellInfo.rowId, 0), OR_PREFIX, excludedNodes);

        int currentResultNum = results.size();
        chooseTargetSpatial(currentResultNum + 1, excludedNodes, softExcludedNodes, results,
            avoidStaleNodes, maxNodesPerRack, storageTypes, blocksize, cellIndexInfo.filename);
        saveGroupReplicaPosToFile(results.get(currentResultNum).getDatanodeDescriptor(), rightGroupCellInfo,
            MG_PREFIX, groupInfoFile);
      } else {
        DatanodeDescriptor node = (DatanodeDescriptor) clusterMap.getNode(rightGroupReplicaPos);
        DatanodeStorageInfo storageInfo = getDatanodeStorageInfo(node);
        results.add(storageInfo);
        excludedNodes.add(node);
        storageTypes.put(storageInfo.getStorageType(), storageTypes.get(storageInfo.getStorageType()) - 1);
      }
    }


    // overlapped row
    if (groupCellInfo.ORCoord != null) {
      String replicaPos = readGroupReplicaPosFromFile(groupInfoFile, groupCellInfo.ORCoord, OR_PREFIX);
      DatanodeDescriptor node = (DatanodeDescriptor) clusterMap.getNode(replicaPos);
      DatanodeStorageInfo storageInfo = getDatanodeStorageInfo(node);
      results.add(storageInfo);
      excludedNodes.add(node);
      storageTypes.put(storageInfo.getStorageType(), storageTypes.get(storageInfo.getStorageType()) - 1);
    }


    int testInt = results.size();

    chooseTargetSpatial(3, excludedNodes, softExcludedNodes, results, avoidStaleNodes,
        maxNodesPerRack, storageTypes, blocksize, cellIndexInfo.filename);

    LinkedHashSet<DatanodeStorageInfo> resultSet = new LinkedHashSet<>(results);

    return resultSet.toArray(new DatanodeStorageInfo[0]);

  }

  private void findAndAddToExcludedNode(String groupInfoFile, Coord toAddNode, String prefix, Set<Node> excludedNodes) {
    String pos = readGroupReplicaPosFromFile(groupInfoFile, toAddNode, prefix);
    if (pos != null) {
      DatanodeDescriptor node = (DatanodeDescriptor) clusterMap.getNode(pos);
      excludedNodes.add(node);
    }
  }


  /**
   * choose a random datanode in the other rack of first replica
   * load balan
   */
  DatanodeStorageInfo chooseRandomInOtherRackSpatial(
      DatanodeDescriptor firstReplicaNode,
      final Set<Node> excludedNodes,
      final Set<Node> softExcludedNodes,
      final List<DatanodeStorageInfo> results,
      final boolean avoidStaleNodes,
      final int maxNodesPerRack,
      EnumMap<StorageType, Integer> storageTypes,
      final long blocksize
  ) {
    // 与第一个副本不同的rack
    DatanodeStorageInfo info = chooseRandomSpatial("~" + firstReplicaNode.getNetworkLocation(),
        excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes, softExcludedNodes);
    if (info == null) {
      info = chooseRandomSpatial(NodeBase.ROOT, excludedNodes, blocksize, maxNodesPerRack,
          results, avoidStaleNodes, storageTypes, softExcludedNodes);
    }
    return info;
  }


  /**
   * Randomly choose one target from the given <i>scope</i>.
   * <p>
   * add softExcludedNodes for spatial limitation
   *
   * @return the chosen storage, if there is any.
   * <p>
   * return value is not vary important, for the chosen storage has been add to excludedNodes and results!!!!
   */
  protected DatanodeStorageInfo chooseRandomSpatial(String scope,
                                                    Set<Node> excludedNodes,
                                                    long blocksize,
                                                    int maxNodesPerRack,
                                                    List<DatanodeStorageInfo> results,
                                                    boolean avoidStaleNodes,
                                                    EnumMap<StorageType, Integer> storageTypes,
                                                    Set<Node> softExcludedNodes) {
    //          throws NotEnoughReplicasException {

    LinkedHashSet<Node> tmpSoftExcludedNodes = new LinkedHashSet<>(softExcludedNodes);
    DatanodeStorageInfo result = null;

    // 当无法满足要求时，降低softExcludedNodes的限制
    do {
      Set<Node> allExcludedNodes = new TreeSet<>();
      allExcludedNodes.addAll(excludedNodes);
      allExcludedNodes.addAll(tmpSoftExcludedNodes);
      try {
        // chooseRandom 的返回值没什么用，结果已经加入到results和excludedNodes里了....
        result = chooseRandom(1, scope, allExcludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes);
        break;
      } catch (NotEnoughReplicasException e) {
        //  avoid unbalanced problem when different number in each rack
        if (isBalanceUpload && scope.startsWith("~")) {
          break;
        }
        if (!tmpSoftExcludedNodes.isEmpty()) {
          tmpSoftExcludedNodes.remove(tmpSoftExcludedNodes.toArray(new Node[0])[tmpSoftExcludedNodes.size() - 1]);
        }
      }

    } while (!tmpSoftExcludedNodes.isEmpty());

    if (result != null) {
      excludedNodes.add(result.getDatanodeDescriptor());
    }

    return result;
  }


  // writer 的问题暂不考虑
  // excludedNodes contain choosenNodes and other unwanted nodes(maybe a node not contain proper storage, not
  // consider now)
  // softExcludedNodes contain block positions of previous grid node, may be removed if cluster condition restricts.
  // writer is not considered for load balancing
  // 第一个副本直接传入(左方点的第二个副本）
  // 第二个副本放在不同于第一个副本rack的节点 （左方两个grid node所在的副本加入到excludedNodes内，如果节点数量不足，使用set最右边的节点）
  // 第三个副本放在第二个副本同rack的节点
  boolean chooseTargetSpatial(int numOfReplicas,
                              final Set<Node> excludedNodes,
                              final Set<Node> softExcludedNodes,
                              final List<DatanodeStorageInfo> results,
                              final boolean avoidStaleNodes,
                              final int maxNodesPerRack,
                              EnumMap<StorageType, Integer> storageTypes,
                              final long blocksize,
                              String previousBlockFilename
  ) {
    int resultNumber = results.size();
    int additionalNumOfReplicas = numOfReplicas - resultNumber;
    if (additionalNumOfReplicas <= 0) {
      return true;
    }

    for (int i = 0; i < results.size(); i++)
      excludedNodes.add(results.get(i).getDatanodeDescriptor());


    if (resultNumber == 0) {
      // 与第一个副本不同的rack
      DatanodeStorageInfo info = chooseRandomSpatial(NodeBase.ROOT, excludedNodes, blocksize, maxNodesPerRack,
          results, avoidStaleNodes, storageTypes, softExcludedNodes);
      if (info == null) {
        return false;
      }
      if (--additionalNumOfReplicas <= 0) {
        return true;
      }
    }


    Node firstReplicaNode = results.get(0).getDatanodeDescriptor();

    // 第二个副本
    if (resultNumber <= 1) {

      if (isBalanceUpload) {
        String[] splitDatanodes = readSplitPositionsFromFile(GROUP_INFO_FILE_PATH + "/" + previousBlockFilename);
        HashMap<String, Integer> nodeDataCount = new HashMap<>();
        for (Node n : clusterMap.getLeaves(NodeBase.ROOT)) {
          String nodePos = n.getNetworkLocation() + "/" + n.getName();
          nodeDataCount.put(nodePos, 0);
        }
        for (String splitDatanode : splitDatanodes) {
          if (nodeDataCount.containsKey(splitDatanode)) {
            nodeDataCount.put(splitDatanode, nodeDataCount.get(splitDatanode) + 1);
          } else {
            nodeDataCount.put(splitDatanode, 1);
          }
        }

        // sort by the replica number the host has
        LinkedList<Map.Entry<String, Integer>> tempList = new LinkedList<>(nodeDataCount.entrySet());
        Collections.sort(tempList, new Comparator<Map.Entry<String, Integer>>() {
          @Override
          public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
            return o1.getValue().compareTo(o2.getValue()) * (-1);
          }
        });

        for (int i = 0; i < tempList.size(); i++) {
          if (tempList.get(i).getValue() > tempList.getLast().getValue()) {
            softExcludedNodes.add(clusterMap.getNode(tempList.get(i).getKey()));
          } else {
            break;
          }
        }
      }

      // 与第一个副本不同的rack
      DatanodeStorageInfo info = chooseRandomSpatial("~" + firstReplicaNode.getNetworkLocation(),
          excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes, softExcludedNodes);
      if (info == null) {
        info = chooseRandomSpatial(NodeBase.ROOT, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes, softExcludedNodes);
      }
      if (info == null) {
        return false;
      }
      if (--additionalNumOfReplicas <= 0) {
        return true;
      }
    }

    // 第三个副本
    if (resultNumber <= 2) {
      String secondReplicaRack = results.get(1).getDatanodeDescriptor().getNetworkLocation();
      DatanodeStorageInfo info = chooseRandomSpatial(secondReplicaRack, excludedNodes, blocksize, maxNodesPerRack,
          results, avoidStaleNodes,
          storageTypes, softExcludedNodes);
      if (info == null) {
        info = chooseRandomSpatial(NodeBase.ROOT, excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes,
            storageTypes, softExcludedNodes);
      }
      if (info == null) {
        return false;
      }
      if (--additionalNumOfReplicas <= 0) {
        return true;
      }
    }


    while (additionalNumOfReplicas != 0) {
      DatanodeStorageInfo info = chooseRandomSpatial(NodeBase.ROOT, excludedNodes, blocksize, maxNodesPerRack, results,
          avoidStaleNodes,
          storageTypes, softExcludedNodes);
      if (info == null) {
        return false;
      }
      additionalNumOfReplicas--;
    }

    return true;

  }

  /**
   * TODO  考虑具体细节
   * 从datanode上选择storageInfo
   *
   * @param dd datanode
   * @return storageInfo which is
   */
  // TODO consider datanodeStorageInfo
  DatanodeStorageInfo getDatanodeStorageInfo(DatanodeDescriptor dd) {
    return dd.getStorageInfos()[0];
  }


  /**
   * read split positions from file
   * every four adjacent cell will store in a datanode
   * <p>
   * used for data balancing
   *
   * @return all split position of uploaded file
   */
  String[] readSplitPositionsFromFile(String file) {
    ArrayList<String> datanodeLocs = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line = br.readLine();
      while (line != null) {
        String[] lineSplit = line.split(" ");
        Long lineRowId = Long.parseLong(lineSplit[0]);
        Long lineColId = Long.parseLong(lineSplit[1]);
        if (-1 == lineRowId && -1 == lineColId) {
          datanodeLocs.add(lineSplit[2]);
        }
        line = br.readLine();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return datanodeLocs.toArray(new String[0]);
  }


  // 带faveredNodes的函数
  @Override
  DatanodeStorageInfo[] chooseTarget(String src,
                                     int numOfReplicas,
                                     Node writer,
                                     Set<Node> excludedNodes,
                                     long blocksize,
                                     List<DatanodeDescriptor> favoredNodes,
                                     BlockStoragePolicy storagePolicy) {
    try {
      if (favoredNodes == null || favoredNodes.size() == 0) {
        // Favored nodes not specified, fall back to regular block placement.
        return chooseTarget(src, numOfReplicas, writer,
            new ArrayList<DatanodeStorageInfo>(numOfReplicas), false,
            excludedNodes, blocksize, storagePolicy);
      }

      Set<Node> favoriteAndExcludedNodes = excludedNodes == null ?
          new HashSet<Node>() : new HashSet<>(excludedNodes);
      final List<StorageType> requiredStorageTypes = storagePolicy
          .chooseStorageTypes((short) numOfReplicas);
      final EnumMap<StorageType, Integer> storageTypes =
          getRequiredStorageTypes(requiredStorageTypes);

      // Choose favored nodes
      List<DatanodeStorageInfo> results = new ArrayList<DatanodeStorageInfo>();
      boolean avoidStaleNodes = stats != null
          && stats.isAvoidingStaleDataNodesForWrite();

      int maxNodesAndReplicas[] = getMaxNodesPerRack(0, numOfReplicas);
      numOfReplicas = maxNodesAndReplicas[0];
      int maxNodesPerRack = maxNodesAndReplicas[1];

      for (int i = 0; i < favoredNodes.size() && results.size() < numOfReplicas; i++) {
        DatanodeDescriptor favoredNode = favoredNodes.get(i);
        // Choose a single node which is local to favoredNode.
        // 'results' is updated within chooseLocalNode
        final DatanodeStorageInfo target = chooseLocalStorage(favoredNode,
            favoriteAndExcludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes, false);
        if (target == null) {
          LOG.warn("Could not find a target for file " + src
              + " with favored node " + favoredNode);
          continue;
        }
        favoriteAndExcludedNodes.add(target.getDatanodeDescriptor());
      }

      if (results.size() < numOfReplicas) {
        // Not enough favored nodes, choose other nodes.
        numOfReplicas -= results.size();
        DatanodeStorageInfo[] remainingTargets =
            chooseTarget(src, numOfReplicas, writer, results,
                false, favoriteAndExcludedNodes, blocksize, storagePolicy);
        for (int i = 0; i < remainingTargets.length; i++) {
          results.add(remainingTargets[i]);
        }
      }
      return getPipeline(writer,
          results.toArray(new DatanodeStorageInfo[0]));
    } catch (NotEnoughReplicasException nr) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to choose with favored nodes (=" + favoredNodes
            + "), disregard favored nodes hint and retry.", nr);
      }
      // Fall back to regular block placement disregarding favored nodes hint
      return chooseTarget(src, numOfReplicas, writer,
          new ArrayList<DatanodeStorageInfo>(numOfReplicas), false,
          excludedNodes, blocksize, storagePolicy);
    }
  }

  /**
   * This is the implementation.
   */
  private DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
                                             Node writer,
                                             List<DatanodeStorageInfo> chosenStorage,
                                             boolean returnChosenNodes,
                                             Set<Node> excludedNodes,
                                             long blocksize,
                                             final BlockStoragePolicy storagePolicy) {
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves() == 0) {
      return DatanodeStorageInfo.EMPTY_ARRAY;
    }

    if (excludedNodes == null) {
      excludedNodes = new HashSet<Node>();
    }

    int[] result = getMaxNodesPerRack(chosenStorage.size(), numOfReplicas);
    numOfReplicas = result[0];
    int maxNodesPerRack = result[1];

    final List<DatanodeStorageInfo> results = new ArrayList<DatanodeStorageInfo>(chosenStorage);
    for (DatanodeStorageInfo storage : chosenStorage) {
      // add localMachine and related nodes to excludedNodes
      addToExcludedNodes(storage.getDatanodeDescriptor(), excludedNodes);
    }

    boolean avoidStaleNodes = (stats != null
        && stats.isAvoidingStaleDataNodesForWrite());
    final Node localNode = chooseTargetSpatial(numOfReplicas, writer, excludedNodes,
        blocksize, maxNodesPerRack, results, avoidStaleNodes, storagePolicy,
        EnumSet.noneOf(StorageType.class), results.isEmpty());
    if (!returnChosenNodes) {
      results.removeAll(chosenStorage);
    }

    // sorting nodes to form a pipeline
    return getPipeline(
        (writer instanceof DatanodeDescriptor) ? writer
            : localNode,
        results.toArray(new DatanodeStorageInfo[0]));
  }

  /**
   * Calculate the maximum number of replicas to allocate per rack. It also
   * limits the total number of replicas to the total number of nodes in the
   * cluster. Caller should adjust the replica count to the return value.
   *
   * @param numOfChosen   The number of already chosen nodes.
   * @param numOfReplicas The number of additional nodes to allocate.
   * @return integer array. Index 0: The number of nodes allowed to allocate
   * in addition to already chosen nodes.
   * Index 1: The maximum allowed number of nodes per rack. This
   * is independent of the number of chosen nodes, as it is calculated
   * using the target number of replicas.
   */
  private int[] getMaxNodesPerRack(int numOfChosen, int numOfReplicas) {
    int clusterSize = clusterMap.getNumOfLeaves();
    int totalNumOfReplicas = numOfChosen + numOfReplicas;
    if (totalNumOfReplicas > clusterSize) {
      numOfReplicas -= (totalNumOfReplicas - clusterSize);
      totalNumOfReplicas = clusterSize;
    }
    // No calculation needed when there is only one rack or picking one node.
    int numOfRacks = clusterMap.getNumOfRacks();
    if (numOfRacks == 1 || totalNumOfReplicas <= 1) {
      return new int[]{numOfReplicas, totalNumOfReplicas};
    }

    int maxNodesPerRack = (totalNumOfReplicas - 1) / numOfRacks + 2;
    // At this point, there are more than one racks and more than one replicas
    // to store. Avoid all replicas being in the same rack.
    //
    // maxNodesPerRack has the following properties at this stage.
    //   1) maxNodesPerRack >= 2
    //   2) (maxNodesPerRack-1) * numOfRacks > totalNumOfReplicas
    //          when numOfRacks > 1
    //
    // Thus, the following adjustment will still result in a value that forces
    // multi-rack allocation and gives enough number of total nodes.
    if (maxNodesPerRack == totalNumOfReplicas) {
      maxNodesPerRack--;
    }
    return new int[]{numOfReplicas, maxNodesPerRack};
  }

  private EnumMap<StorageType, Integer> getRequiredStorageTypes(
      List<StorageType> types) {
    EnumMap<StorageType, Integer> map = new EnumMap<>(StorageType.class);
    for (StorageType type : types) {
      if (!map.containsKey(type)) {
        map.put(type, 1);
      } else {
        int num = map.get(type);
        map.put(type, num + 1);
      }
    }
    return map;
  }

  /**
   * choose <i>numOfReplicas</i> from all data nodes
   *
   * @param numOfReplicas   additional number of replicas wanted
   * @param writer          the writer's machine, could be a non-DatanodeDescriptor node
   * @param excludedNodes   datanodes that should not be considered as targets
   * @param blocksize       size of the data to be written
   * @param maxNodesPerRack max nodes allowed per rack
   * @param results         the target nodes already chosen
   * @param avoidStaleNodes avoid stale nodes in replica choosing
   * @return local node of writer (not chosen node)
   */
  private Node chooseTargetSpatial(int numOfReplicas,
                                   Node writer,
                                   final Set<Node> excludedNodes,
                                   final long blocksize,
                                   final int maxNodesPerRack,
                                   final List<DatanodeStorageInfo> results,
                                   final boolean avoidStaleNodes,
                                   final BlockStoragePolicy storagePolicy,
                                   final EnumSet<StorageType> unavailableStorages,
                                   final boolean newBlock) {
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves() == 0) {
      return (writer instanceof DatanodeDescriptor) ? writer : null;
    }
    final int numOfResults = results.size();
    final int totalReplicasExpected = numOfReplicas + numOfResults;
    if ((!(writer instanceof DatanodeDescriptor)) && !newBlock) {
      writer = results.get(0).getDatanodeDescriptor();
    }

    // Keep a copy of original excludedNodes
    final Set<Node> oldExcludedNodes = new HashSet<>(excludedNodes);

    // choose storage types; use fallbacks for unavailable storages
    final List<StorageType> requiredStorageTypes = storagePolicy
        .chooseStorageTypes((short) totalReplicasExpected,
            DatanodeStorageInfo.toStorageTypes(results),
            unavailableStorages, newBlock);
    final EnumMap<StorageType, Integer> storageTypes =
        getRequiredStorageTypes(requiredStorageTypes);
    if (LOG.isTraceEnabled()) {
      LOG.trace("storageTypes=" + storageTypes);
    }

    try {
      if ((numOfReplicas = requiredStorageTypes.size()) == 0) {
        throw new NotEnoughReplicasException(
            "All required storage types are unavailable: "
                + " unavailableStorages=" + unavailableStorages
                + ", storagePolicy=" + storagePolicy);
      }

      if (numOfResults == 0) {
        writer = chooseLocalStorage(writer, excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes, storageTypes, true)
            .getDatanodeDescriptor();
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      final DatanodeDescriptor dn0 = results.get(0).getDatanodeDescriptor();
      if (numOfResults <= 1) {
        chooseRemoteRack(1, dn0, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes);
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      if (numOfResults <= 2) {
        final DatanodeDescriptor dn1 = results.get(1).getDatanodeDescriptor();
        if (clusterMap.isOnSameRack(dn0, dn1)) {
          chooseRemoteRack(1, dn0, excludedNodes, blocksize, maxNodesPerRack,
              results, avoidStaleNodes, storageTypes);
        } else if (newBlock) {
          chooseLocalRack(dn1, excludedNodes, blocksize, maxNodesPerRack,
              results, avoidStaleNodes, storageTypes);
        } else {
          chooseLocalRack(writer, excludedNodes, blocksize, maxNodesPerRack,
              results, avoidStaleNodes, storageTypes);
        }
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      chooseRandom(numOfReplicas, NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      final String message = "Failed to place enough replicas, still in need of "
          + (totalReplicasExpected - results.size()) + " to reach "
          + totalReplicasExpected
          + " (unavailableStorages=" + unavailableStorages
          + ", storagePolicy=" + storagePolicy
          + ", newBlock=" + newBlock + ")";

      if (LOG.isTraceEnabled()) {
        LOG.trace(message, e);
      } else {
        LOG.warn(message + " " + e.getMessage());
      }

      if (avoidStaleNodes) {
        // Retry chooseTargetSpatial again, this time not avoiding stale nodes.

        // excludedNodes contains the initial excludedNodes and nodes that were
        // not chosen because they were stale, decommissioned, etc.
        // We need to additionally exclude the nodes that were added to the
        // result list in the successful calls to choose*() above.
        for (DatanodeStorageInfo resultStorage : results) {
          addToExcludedNodes(resultStorage.getDatanodeDescriptor(), oldExcludedNodes);
        }
        // Set numOfReplicas, since it can get out of sync with the result list
        // if the NotEnoughReplicasException was thrown in chooseRandom().
        numOfReplicas = totalReplicasExpected - results.size();
        return chooseTargetSpatial(numOfReplicas, writer, oldExcludedNodes, blocksize,
            maxNodesPerRack, results, false, storagePolicy, unavailableStorages,
            newBlock);
      }

      boolean retry = false;
      // simply add all the remaining types into unavailableStorages and give
      // another try. No best effort is guaranteed here.
      for (StorageType type : storageTypes.keySet()) {
        if (!unavailableStorages.contains(type)) {
          unavailableStorages.add(type);
          retry = true;
        }
      }
      if (retry) {
        for (DatanodeStorageInfo resultStorage : results) {
          addToExcludedNodes(resultStorage.getDatanodeDescriptor(),
              oldExcludedNodes);
        }
        numOfReplicas = totalReplicasExpected - results.size();
        return chooseTargetSpatial(numOfReplicas, writer, oldExcludedNodes, blocksize,
            maxNodesPerRack, results, false, storagePolicy, unavailableStorages,
            newBlock);
      }
    }
    return writer;
  }

  /**
   * Choose <i>localMachine</i> as the target.
   * if <i>localMachine</i> is not available,
   * choose a node on the same rack
   *
   * @return the chosen storage
   */
  protected DatanodeStorageInfo chooseLocalStorage(Node localMachine,
                                                   Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
                                                   List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
                                                   EnumMap<StorageType, Integer> storageTypes,
                                                   boolean fallbackToLocalRack)
      throws NotEnoughReplicasException {
    // if no local machine, randomly choose one node
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
    if (preferLocalNode && localMachine instanceof DatanodeDescriptor) {
      DatanodeDescriptor localDatanode = (DatanodeDescriptor) localMachine;
      // otherwise try local machine first
      if (excludedNodes.add(localMachine)) { // was not in the excluded list
        for (Iterator<Map.Entry<StorageType, Integer>> iter = storageTypes
            .entrySet().iterator(); iter.hasNext(); ) {
          Map.Entry<StorageType, Integer> entry = iter.next();
          for (DatanodeStorageInfo localStorage : DFSUtil.shuffle(
              localDatanode.getStorageInfos())) {
            StorageType type = entry.getKey();
            if (addIfIsGoodTarget(localStorage, excludedNodes, blocksize,
                maxNodesPerRack, false, results, avoidStaleNodes, type) >= 0) {
              int num = entry.getValue();
              if (num == 1) {
                iter.remove();
              } else {
                entry.setValue(num - 1);
              }
              return localStorage;
            }
          }
        }
      }
    }

    if (!fallbackToLocalRack) {
      return null;
    }
    // try a node on local rack
    return chooseLocalRack(localMachine, excludedNodes, blocksize,
        maxNodesPerRack, results, avoidStaleNodes, storageTypes);
  }

  /**
   * Add <i>localMachine</i> and related nodes to <i>excludedNodes</i>
   * for next replica choosing. In sub class, we can add more nodes within
   * the same failure domain of localMachine
   *
   * @return number of new excluded nodes
   */
  protected int addToExcludedNodes(DatanodeDescriptor localMachine,
                                   Set<Node> excludedNodes) {
    return excludedNodes.add(localMachine) ? 1 : 0;
  }

  /**
   * Choose one node from the rack that <i>localMachine</i> is on.
   * if no such node is available, choose one node from the rack where
   * a second replica is on.
   * if still no such node is available, choose a random node
   * in the cluster.
   *
   * @return the chosen node
   */
  protected DatanodeStorageInfo chooseLocalRack(Node localMachine,
                                                Set<Node> excludedNodes,
                                                long blocksize,
                                                int maxNodesPerRack,
                                                List<DatanodeStorageInfo> results,
                                                boolean avoidStaleNodes,
                                                EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    // no local machine, so choose a random machine
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
    final String localRack = localMachine.getNetworkLocation();

    try {
      // choose one from the local rack
      return chooseRandom(localRack, excludedNodes,
          blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      // find the next replica and retry with its rack
      for (DatanodeStorageInfo resultStorage : results) {
        DatanodeDescriptor nextNode = resultStorage.getDatanodeDescriptor();
        if (nextNode != localMachine) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Failed to choose from local rack (location = " + localRack
                + "), retry with the rack of the next replica (location = "
                + nextNode.getNetworkLocation() + ")", e);
          }
          return chooseFromNextRack(nextNode, excludedNodes, blocksize,
              maxNodesPerRack, results, avoidStaleNodes, storageTypes);
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to choose from local rack (location = " + localRack
            + "); the second replica is not found, retry choosing ramdomly", e);
      }
      //the second replica is not found, randomly choose one from the network
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  private DatanodeStorageInfo chooseFromNextRack(Node next,
                                                 Set<Node> excludedNodes,
                                                 long blocksize,
                                                 int maxNodesPerRack,
                                                 List<DatanodeStorageInfo> results,
                                                 boolean avoidStaleNodes,
                                                 EnumMap<StorageType, Integer> storageTypes) throws NotEnoughReplicasException {
    final String nextRack = next.getNetworkLocation();
    try {
      return chooseRandom(nextRack, excludedNodes, blocksize, maxNodesPerRack,
          results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to choose from the next rack (location = " + nextRack
            + "), retry choosing ramdomly", e);
      }
      //otherwise randomly choose one from the network
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  /**
   * Choose <i>numOfReplicas</i> nodes from the racks
   * that <i>localMachine</i> is NOT on.
   * if not enough nodes are available, choose the remaining ones
   * from the local rack
   */

  protected void chooseRemoteRack(int numOfReplicas,
                                  DatanodeDescriptor localMachine,
                                  Set<Node> excludedNodes,
                                  long blocksize,
                                  int maxReplicasPerRack,
                                  List<DatanodeStorageInfo> results,
                                  boolean avoidStaleNodes,
                                  EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    int oldNumOfReplicas = results.size();
    // randomly choose one node from remote racks
    try {
      chooseRandom(numOfReplicas, "~" + localMachine.getNetworkLocation(),
          excludedNodes, blocksize, maxReplicasPerRack, results,
          avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to choose remote rack (location = ~"
            + localMachine.getNetworkLocation() + "), fallback to local rack", e);
      }
      chooseRandom(numOfReplicas - (results.size() - oldNumOfReplicas),
          localMachine.getNetworkLocation(), excludedNodes, blocksize,
          maxReplicasPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  /**
   * Randomly choose one target from the given <i>scope</i>.
   *
   * @return the chosen storage, if there is any.
   */
  protected DatanodeStorageInfo chooseRandom(String scope,
                                             Set<Node> excludedNodes,
                                             long blocksize,
                                             int maxNodesPerRack,
                                             List<DatanodeStorageInfo> results,
                                             boolean avoidStaleNodes,
                                             EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    return chooseRandom(1, scope, excludedNodes, blocksize, maxNodesPerRack,
        results, avoidStaleNodes, storageTypes);
  }

  /**
   * Randomly choose <i>numOfReplicas</i> targets from the given <i>scope</i>.
   *
   * @return the first chosen node, if there is any.
   */
  protected DatanodeStorageInfo chooseRandom(int numOfReplicas,
                                             String scope,
                                             Set<Node> excludedNodes,
                                             long blocksize,
                                             int maxNodesPerRack,
                                             List<DatanodeStorageInfo> results,
                                             boolean avoidStaleNodes,
                                             EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {

    int numOfAvailableNodes = clusterMap.countNumOfAvailableNodes(
        scope, excludedNodes);
    int refreshCounter = numOfAvailableNodes;
    StringBuilder builder = null;
    if (LOG.isDebugEnabled()) {
      builder = debugLoggingBuilder.get();
      builder.setLength(0);
      builder.append("[");
    }
    boolean badTarget = false;
    DatanodeStorageInfo firstChosen = null;
    while (numOfReplicas > 0 && numOfAvailableNodes > 0) {
      DatanodeDescriptor chosenNode = chooseDataNode(scope);
      if (excludedNodes.add(chosenNode)) { //was not in the excluded list
        if (LOG.isDebugEnabled() && builder != null) {
          builder.append("\nNode ").append(NodeBase.getPath(chosenNode)).append(" [");
        }
        numOfAvailableNodes--;

        final DatanodeStorageInfo[] storages = DFSUtil.shuffle(
            chosenNode.getStorageInfos());
        int i = 0;
        boolean search = true;
        for (Iterator<Map.Entry<StorageType, Integer>> iter = storageTypes
            .entrySet().iterator(); search && iter.hasNext(); ) {
          Map.Entry<StorageType, Integer> entry = iter.next();
          for (i = 0; i < storages.length; i++) {
            StorageType type = entry.getKey();
            final int newExcludedNodes = addIfIsGoodTarget(storages[i],
                excludedNodes, blocksize, maxNodesPerRack, considerLoad, results,
                avoidStaleNodes, type);
            if (newExcludedNodes >= 0) {
              numOfReplicas--;
              if (firstChosen == null) {
                firstChosen = storages[i];
              }
              numOfAvailableNodes -= newExcludedNodes;
              int num = entry.getValue();
              if (num == 1) {
                iter.remove();
              } else {
                entry.setValue(num - 1);
              }
              search = false;
              break;
            }
          }
        }
        if (LOG.isDebugEnabled() && builder != null) {
          builder.append("\n]");
        }


        // If no candidate storage was found on this DN then set badTarget.
        badTarget = (i == storages.length);
      }
      // Refresh the node count. If the live node count became smaller,
      // but it is not reflected in this loop, it may loop forever in case
      // the replicas/rack cannot be satisfied.
      if (--refreshCounter == 0) {
        numOfAvailableNodes = clusterMap.countNumOfAvailableNodes(scope,
            excludedNodes);
        refreshCounter = numOfAvailableNodes;
      }
    }

    if (numOfReplicas > 0) {
      String detail = enableDebugLogging;
      if (LOG.isDebugEnabled()) {
        if (badTarget && builder != null) {
          detail = builder.toString();
          builder.setLength(0);
        } else {
          detail = "";
        }
      }
      throw new NotEnoughReplicasException(detail);
    }

    return firstChosen;
  }

  /**
   * Choose a datanode from the given <i>scope</i>.
   *
   * @return the chosen node, if there is any.
   */
  protected DatanodeDescriptor chooseDataNode(final String scope) {
    return (DatanodeDescriptor) clusterMap.chooseRandom(scope);
  }


  /**
   * If the given storage is a good target, add it to the result list and
   * update the set of excluded nodes.
   *
   * @return -1 if the given is not a good target;
   * otherwise, return the number of nodes added to excludedNodes set.
   */
  int addIfIsGoodTarget(DatanodeStorageInfo storage,
                        Set<Node> excludedNodes,
                        long blockSize,
                        int maxNodesPerRack,
                        boolean considerLoad,
                        List<DatanodeStorageInfo> results,
                        boolean avoidStaleNodes,
                        StorageType storageType) {
    if (isGoodTarget(storage, blockSize, maxNodesPerRack, considerLoad,
        results, avoidStaleNodes, storageType)) {
      results.add(storage);
      // add node and related nodes to excludedNode
      return addToExcludedNodes(storage.getDatanodeDescriptor(), excludedNodes);
    } else {
      return -1;
    }
  }

  private static void logNodeIsNotChosen(DatanodeStorageInfo storage, String reason) {
    if (LOG.isDebugEnabled()) {
      // build the error message for later use.
      debugLoggingBuilder.get()
          .append("\n  Storage ").append(storage)
          .append(" is not chosen since ").append(reason).append(".");
    }
  }

  /**
   * Determine if a storage is a good target.
   *
   * @param storage          The target storage
   * @param blockSize        Size of block
   * @param maxTargetPerRack Maximum number of targets per rack. The value of
   *                         this parameter depends on the number of racks in
   *                         the cluster and total number of replicas for a block
   * @param considerLoad     whether or not to consider load of the target node
   * @param results          A list containing currently chosen nodes. Used to check if
   *                         too many nodes has been chosen in the target rack.
   * @param avoidStaleNodes  Whether or not to avoid choosing stale nodes
   * @return Return true if <i>node</i> has enough space,
   * does not have too much load,
   * and the rack does not have too many nodes.
   */
  private boolean isGoodTarget(DatanodeStorageInfo storage,
                               long blockSize, int maxTargetPerRack,
                               boolean considerLoad,
                               List<DatanodeStorageInfo> results,
                               boolean avoidStaleNodes,
                               StorageType requiredStorageType) {
    if (storage.getStorageType() != requiredStorageType) {
      logNodeIsNotChosen(storage, "storage types do not match,"
          + " where the required storage type is " + requiredStorageType);
      return false;
    }
    if (storage.getState() == State.READ_ONLY_SHARED) {
      logNodeIsNotChosen(storage, "storage is read-only");
      return false;
    }

    if (storage.getState() == State.FAILED) {
      logNodeIsNotChosen(storage, "storage has failed");
      return false;
    }

    DatanodeDescriptor node = storage.getDatanodeDescriptor();
    // check if the node is (being) decommissioned
    if (node.isDecommissionInProgress() || node.isDecommissioned()) {
      logNodeIsNotChosen(storage, "the node is (being) decommissioned ");
      return false;
    }

    if (avoidStaleNodes) {
      if (node.isStale(this.staleInterval)) {
        logNodeIsNotChosen(storage, "the node is stale ");
        return false;
      }
    }

    final long requiredSize = blockSize * HdfsConstants.MIN_BLOCKS_FOR_WRITE;
    final long scheduledSize = blockSize * node.getBlocksScheduled(storage.getStorageType());
    final long remaining = node.getRemaining(storage.getStorageType(),
        requiredSize);
    if (requiredSize > remaining - scheduledSize) {
      logNodeIsNotChosen(storage, "the node does not have enough "
          + storage.getStorageType() + " space"
          + " (required=" + requiredSize
          + ", scheduled=" + scheduledSize
          + ", remaining=" + remaining + ")");
      return false;
    }

    // check the communication traffic of the target machine
    if (considerLoad) {
      final double maxLoad = 2.0 * stats.getInServiceXceiverAverage();
      final int nodeLoad = node.getXceiverCount();
      if (nodeLoad > maxLoad) {
        logNodeIsNotChosen(storage, "the node is too busy (load: " + nodeLoad
            + " > " + maxLoad + ") ");
        return false;
      }
    }

    // check if the target rack has chosen too many nodes
    String rackname = node.getNetworkLocation();
    int counter = 1;
    for (DatanodeStorageInfo resultStorage : results) {
      if (rackname.equals(
          resultStorage.getDatanodeDescriptor().getNetworkLocation())) {
        counter++;
      }
    }
    if (counter > maxTargetPerRack) {
      logNodeIsNotChosen(storage, "the rack has too many chosen nodes ");
      return false;
    }
    return true;
  }

  /**
   * Return a pipeline of nodes.
   * The pipeline is formed finding a shortest path that
   * starts from the writer and traverses all <i>nodes</i>
   * This is basically a traveling salesman problem.
   */
  private DatanodeStorageInfo[] getPipeline(Node writer,
                                            DatanodeStorageInfo[] storages) {
    if (storages.length == 0) {
      return storages;
    }

    synchronized (clusterMap) {
      int index = 0;
      if (writer == null || !clusterMap.contains(writer)) {
        writer = storages[0].getDatanodeDescriptor();
      }
      for (; index < storages.length; index++) {
        DatanodeStorageInfo shortestStorage = storages[index];
        int shortestDistance = clusterMap.getDistance(writer,
            shortestStorage.getDatanodeDescriptor());
        int shortestIndex = index;
        for (int i = index + 1; i < storages.length; i++) {
          int currentDistance = clusterMap.getDistance(writer,
              storages[i].getDatanodeDescriptor());
          if (shortestDistance > currentDistance) {
            shortestDistance = currentDistance;
            shortestStorage = storages[i];
            shortestIndex = i;
          }
        }
        //switch position index & shortestIndex
        if (index != shortestIndex) {
          storages[shortestIndex] = storages[index];
          storages[index] = shortestStorage;
        }
        writer = shortestStorage.getDatanodeDescriptor();
      }
    }
    return storages;
  }

  @Override
  public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs,
                                                   int numberOfReplicas) {
    if (locs == null) {
      locs = DatanodeDescriptor.EMPTY_ARRAY;
    }
    if (!clusterMap.hasClusterEverBeenMultiRack()) {
      // only one rack
      return new BlockPlacementStatusDefault(1, 1);
    }
    int minRacks = 2;
    minRacks = Math.min(minRacks, numberOfReplicas);
    // 1. Check that all locations are different.
    // 2. Count locations on different racks.
    Set<String> racks = new TreeSet<>();
    for (DatanodeInfo dn : locs)
      racks.add(dn.getNetworkLocation());
    return new BlockPlacementStatusDefault(racks.size(), minRacks);
  }

  /**
   * Decide whether deleting the specified replica of the block still makes
   * the block conform to the configured block placement policy.
   *
   * @param moreThanOne The replica locations of this block that are present
   *                    on more than one unique racks.
   * @param exactlyOne  Replica locations of this block that  are present
   *                    on exactly one unique racks.
   * @param excessTypes The excess {@link StorageType}s according to the
   *                    {@link BlockStoragePolicy}.
   * @return the replica that is the best candidate for deletion
   */
  @VisibleForTesting
  public DatanodeStorageInfo chooseReplicaToDelete(
      Collection<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      final List<StorageType> excessTypes,
      Map<String, List<DatanodeStorageInfo>> rackMap) {
    long oldestHeartbeat =
        monotonicNow() - heartbeatInterval * tolerateHeartbeatMultiplier;
    DatanodeStorageInfo oldestHeartbeatStorage = null;
    long minSpace = Long.MAX_VALUE;
    DatanodeStorageInfo minSpaceStorage = null;

    // Pick the node with the oldest heartbeat or with the least free space,
    // if all hearbeats are within the tolerable heartbeat interval
    for (DatanodeStorageInfo storage : pickupReplicaSet(moreThanOne,
        exactlyOne, rackMap)) {
      if (!excessTypes.contains(storage.getStorageType())) {
        continue;
      }

      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      long free = node.getRemaining();
      long lastHeartbeat = node.getLastUpdateMonotonic();
      if (lastHeartbeat < oldestHeartbeat) {
        oldestHeartbeat = lastHeartbeat;
        oldestHeartbeatStorage = storage;
      }
      if (minSpace > free) {
        minSpace = free;
        minSpaceStorage = storage;
      }
    }

    final DatanodeStorageInfo storage;
    if (oldestHeartbeatStorage != null) {
      storage = oldestHeartbeatStorage;
    } else if (minSpaceStorage != null) {
      storage = minSpaceStorage;
    } else {
      return null;
    }
    excessTypes.remove(storage.getStorageType());
    return storage;
  }

  @Override
  public List<DatanodeStorageInfo> chooseReplicasToDelete(
      Collection<DatanodeStorageInfo> candidates,
      int expectedNumOfReplicas,
      List<StorageType> excessTypes,
      DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint) {

    List<DatanodeStorageInfo> excessReplicas = new ArrayList<>();

    final Map<String, List<DatanodeStorageInfo>> rackMap = new HashMap<>();

    final List<DatanodeStorageInfo> moreThanOne = new ArrayList<>();
    final List<DatanodeStorageInfo> exactlyOne = new ArrayList<>();

    // split nodes into two sets
    // moreThanOne contains nodes on rack with more than one replica
    // exactlyOne contains the remaining nodes
    splitNodesWithRack(candidates, rackMap, moreThanOne, exactlyOne);

    // pick one node to delete that favors the delete hint
    // otherwise pick one with least space from priSet if it is not empty
    // otherwise one node with least space from remains
    boolean firstOne = true;
    final DatanodeStorageInfo delNodeHintStorage =
        DatanodeStorageInfo.getDatanodeStorageInfo(candidates, delNodeHint);
    final DatanodeStorageInfo addedNodeStorage =
        DatanodeStorageInfo.getDatanodeStorageInfo(candidates, addedNode);

    while (candidates.size() - expectedNumOfReplicas > excessReplicas.size()) {
      final DatanodeStorageInfo cur;
      if (useDelHint(firstOne, delNodeHintStorage, addedNodeStorage,
          moreThanOne, excessTypes)) {
        cur = delNodeHintStorage;
      } else { // regular excessive replica removal
        cur = chooseReplicaToDelete(moreThanOne, exactlyOne, excessTypes,
            rackMap);
      }
      firstOne = false;
      if (cur == null) {
        LOG.warn("No excess replica can be found. excessTypes: " + excessTypes +
            ". moreThanOne: " + moreThanOne + ". exactlyOne: " + exactlyOne + ".");
        break;
      }

      // adjust rackmap, moreThanOne, and exactlyOne
      adjustSetsWithChosenReplica(rackMap, moreThanOne, exactlyOne, cur);
      excessReplicas.add(cur);
    }
    return excessReplicas;
  }

  /**
   * Check if we can use delHint.
   */
  @VisibleForTesting
  static boolean useDelHint(boolean isFirst, DatanodeStorageInfo delHint,
                            DatanodeStorageInfo added, List<DatanodeStorageInfo> moreThan1Racks,
                            List<StorageType> excessTypes) {
    if (!isFirst) {
      return false; // only consider delHint for the first case
    } else if (delHint == null) {
      return false; // no delHint
    } else if (!excessTypes.contains(delHint.getStorageType())) {
      return false; // delHint storage type is not an excess type
    } else {
      // check if removing delHint reduces the number of racks
      // the added node adds a new rack
      if (moreThan1Racks.contains(delHint)) {
        return true; // delHint and some other nodes are under the same rack
      } else {
        return added != null && !moreThan1Racks.contains(added);
      }
    }
  }

  /**
   * Pick up replica node set for deleting replica as over-replicated.
   * First set contains replica nodes on rack with more than one
   * replica while second set contains remaining replica nodes.
   * If only 1 rack, pick all. If 2 racks, pick all that have more than
   * 1 replicas on the same rack; if no such replicas, pick all.
   * If 3 or more racks, pick all.
   */
  protected Collection<DatanodeStorageInfo> pickupReplicaSet(
      Collection<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      Map<String, List<DatanodeStorageInfo>> rackMap) {
    Collection<DatanodeStorageInfo> ret = new ArrayList<>();
    if (rackMap.size() == 2) {
      for (List<DatanodeStorageInfo> dsi : rackMap.values()) {
        if (dsi.size() >= 2) {
          ret.addAll(dsi);
        }
      }
    }
    if (ret.isEmpty()) {
      // Return all replicas if rackMap.size() != 2
      // or rackMap.size() == 2 but no shared replicas on any rack
      ret.addAll(moreThanOne);
      ret.addAll(exactlyOne);
    }
    return ret;
  }

  @VisibleForTesting
  void setPreferLocalNode(boolean prefer) {
    this.preferLocalNode = prefer;
  }


  //  ---------------------- for spatial group
  // prefix sign, the file will use the prefix in the beginning of line
  final static String MG_PREFIX = "MG"; // main group 的前缀标记
  final static String OC_PREFIX = "OC"; // overlapped column prefix
  final static String OR_PREFIX = "OR"; // overlapped row prefix

  /**
   * save current datanode information of chosen result
   *
   * @param datanodeDescriptor current chosen result
   * @param info               grid cell info
   * @param filepath           file path
   */
  public static void saveGroupReplicaPosToFile(DatanodeDescriptor datanodeDescriptor, GroupCellInfo info,
                                               String groupType,
                                               String filepath) {

    FileOutputStream fileOutputStream;
    try {
      fileOutputStream = new FileOutputStream(new File(filepath), true);
      BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
      bufferedWriter.write(groupType + " ");
      bufferedWriter.write(info.rowId + " ");
      bufferedWriter.write(info.colId + " ");
      bufferedWriter.write(datanodeDescriptor.getNetworkLocation() + "/" +
          datanodeDescriptor.getName());
      bufferedWriter.newLine();
      //      bufferedWriter.write("\n");

      bufferedWriter.flush();
      bufferedWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * input file and info
   * get corresponding datanodeLocations of groupCoord from file
   */
  public static String readGroupReplicaPosFromFile(String file, Coord groupCoord, String type) {

    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line = br.readLine();

      while (line != null) {
        String[] lineSplit = line.split(" ");
        String lineType = lineSplit[0];
        int lineRowId = Integer.parseInt(lineSplit[1]);
        int lineColId = Integer.parseInt(lineSplit[2]);
        if (type.equals(lineType) && groupCoord.rowId == lineRowId && groupCoord.colId == lineColId) {
          return lineSplit[3];
        }
        line = br.readLine();
      }
    } catch (IOException e) {
      //      e.printStackTrace();
      return null;
    }
    return null;
  }

}

