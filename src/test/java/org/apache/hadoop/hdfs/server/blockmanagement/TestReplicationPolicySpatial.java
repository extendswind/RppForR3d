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

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.test.PathUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.util.*;

import static org.junit.Assert.*;

public class TestReplicationPolicySpatial {
  {
    ((Log4JLogger) BlockPlacementPolicy.LOG).getLogger().setLevel(Level.ALL);
  }

  private final Random random = DFSUtil.getRandom();
  private static final int BLOCK_SIZE = 1024;
  private static final int NUM_OF_DATANODES = 19;
  private static NetworkTopology cluster;
  private static NameNode namenode;
  private static BlockPlacementPolicy replicator;
  private static final String filename = "/dummyfile.txt";
  private static DatanodeDescriptor[] dataNodes;
  private static DatanodeStorageInfo[] storages;
  // The interval for marking a datanode as stale,
  private static final long staleInterval =
          DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private static void updateHeartbeatWithUsage(DatanodeDescriptor dn,
                                               long capacity, long dfsUsed, long remaining, long blockPoolUsed,
                                               long dnCacheCapacity, long dnCacheUsed, int xceiverCount, int volFailures) {
    dn.getStorageInfos()[0].setUtilizationForTesting(
            capacity, dfsUsed, remaining, blockPoolUsed);
    dn.updateHeartbeat(
            BlockManagerTestUtil.getStorageReportsForDatanode(dn),
            dnCacheCapacity, dnCacheUsed, xceiverCount, volFailures, null);
  }

  private static void updateHeartbeatForExtraStorage(long capacity,
                                                     long dfsUsed, long remaining, long blockPoolUsed) {
    DatanodeDescriptor dn = dataNodes[5];
    dn.getStorageInfos()[1].setUtilizationForTesting(
            capacity, dfsUsed, remaining, blockPoolUsed);
    dn.updateHeartbeat(
            BlockManagerTestUtil.getStorageReportsForDatanode(dn),
            0L, 0L, 0, 0, null);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    Configuration conf = new HdfsConfiguration();

    // d1 means data center 1, not datanode
    final String[] racks = {
            "/d1/r1",
            "/d1/r1",
            "/d1/r1",
            "/d1/r1",
            "/d1/r1",
            "/d1/r1",
            "/d1/r1",
            "/d1/r2",
            "/d1/r2",
            "/d1/r2",
            "/d1/r2",
            "/d1/r2",
            "/d1/r2",
            "/d1/r2",
            "/d1/r2",
            "/d1/r2",
            "/d1/r2",
            "/d1/r2",
            "/d1/r2"
    };
    storages = DFSTestUtil.createDatanodeStorageInfos(racks);
    dataNodes = DFSTestUtil.toDatanodeDescriptor(storages);

    // create an extra storage for dn5.
    DatanodeStorage extraStorage = new DatanodeStorage(
            storages[5].getStorageID() + "-extra", DatanodeStorage.State.NORMAL,
            StorageType.DEFAULT);
/*    DatanodeStorageInfo si = new DatanodeStorageInfo(
        storages[5].getDatanodeDescriptor(), extraStorage);
*/
    BlockManagerTestUtil.updateStorage(storages[5].getDatanodeDescriptor(),
            extraStorage);

    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    File baseDir = PathUtils.getTestDir(TestReplicationPolicySpatial.class);
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
            new File(baseDir, "name").getPath());

    conf.setBoolean(
            DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, true);
    conf.setBoolean(
            DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, true);
    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY, BlockPlacementPolicyDefaultSpatial.class, BlockPlacementPolicy.class);
//    System.out.println("000000000000000" + conf.get(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY));
    DFSTestUtil.formatNameNode(conf);
    namenode = new NameNode(conf);

    final BlockManager bm = namenode.getNamesystem().getBlockManager();
    replicator = bm.getBlockPlacementPolicy();
    cluster = bm.getDatanodeManager().getNetworkTopology();
    // construct network topology
    for (int i = 0; i < NUM_OF_DATANODES; i++) {
      cluster.add(dataNodes[i]);
      bm.getDatanodeManager().getHeartbeatManager().addDatanode(
              dataNodes[i]);
    }
    resetHeartbeatForStorages();
  }

  private static void resetHeartbeatForStorages() {
    for (int i = 0; i < NUM_OF_DATANODES; i++) {
      updateHeartbeatWithUsage(dataNodes[i],
              2 * HdfsConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0L,
              HdfsConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }
    // No available space in the extra storage of dn0
    updateHeartbeatForExtraStorage(0L, 0L, 0L, 0L);
  }

  private static boolean isOnSameRack(DatanodeStorageInfo left, DatanodeStorageInfo right) {
    return isOnSameRack(left, right.getDatanodeDescriptor());
  }

  private static boolean isOnSameRack(DatanodeStorageInfo left, DatanodeDescriptor right) {
    return cluster.isOnSameRack(left.getDatanodeDescriptor(), right);
  }

//  /**
//   * Test whether the remaining space per storage is individually
//   * considered.
//   */
//  @Test
//  public void testChooseNodeWithMultipleStorages() throws Exception {
//    updateHeartbeatWithUsage(dataNodes[5],
//        2* HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
//        (2* HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE)/3, 0L,
//        0L, 0L, 0, 0);
//
//    updateHeartbeatForExtraStorage(
//        2* HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
//        (2* HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE)/3, 0L);
//
//    DatanodeStorageInfo[] targets;
//    targets = chooseTarget (1, dataNodes[5],
//        new ArrayList<DatanodeStorageInfo>(), null);
//    assertEquals(1, targets.length);
//    assertEquals(storages[4], targets[0]);
//
//    resetHeartbeatForStorages();
//  }

  public void testJustAtest() {
    String filename = "/home/fly/test.dat";
    System.out.println(FilenameUtils.getName(filename));
    System.out.println(FilenameUtils.getBaseName(filename));
    System.out.println(FilenameUtils.getFullPath(filename));

    System.out.println("---------test for showing the cluster map----------");

    BlockPlacementPolicyDefaultSpatial spatialReplicator = (BlockPlacementPolicyDefaultSpatial) replicator;
//    spatialReplicator.tmpPrintDatanodeInfo();

    System.out.println(cluster.getLeaves("").get(0).getNetworkLocation());
    Node node = cluster.getNode("/d1/r1/1.1.1.1:50010");

    System.out.println(node.getName());
    System.out.println(node instanceof DatanodeDescriptor);

  }



  /**
   * In this testcase, client is dataNodes[0]. So the 1st replica should be
   * placed on dataNodes[0], the 2nd replica should be placed on
   * different rack and third should be placed on different node
   * of rack chosen for 2nd node.
   * <p>
   * ChooseTarget() will get a filename, by which determine the block placement.
   * <p>
   * <p>
   * The only excpetion is when the <i>numOfReplicas</i> is 2,
   * the 1st is on dataNodes[0] and the 2nd is on a different rack.
   *
   */
  @Test
  public void testChooseTargetSpatial() {

    System.out.println("\n\n\n -------chooseTargetSpatialGroup------------- \n");

//    updateHeartbeatWithUsage(dataNodes[0],
//            2 * HdfsConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0L,
//            HdfsConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0L,
//            0L, 0L, 4, 0); // overloaded

    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();
//    Set<Node> excludedNodes = null;
    String filename = "grid_test_0_0";
    DatanodeStorageInfo[] targets1 = replicator.chooseTarget(filename, 3, dataNodes[0], chosenNodes, true,
            null, BLOCK_SIZE, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY);

    assertEquals(targets1.length, 3);
    assertEquals(storages[0].getDatanodeDescriptor(), targets1[0].getDatanodeDescriptor());
    assertFalse(isOnSameRack(targets1[0], targets1[1]));
    assertTrue(isOnSameRack(targets1[1], targets1[2]));
    for (DatanodeStorageInfo info : targets1
    ) {
      System.out.println(info.getDatanodeDescriptor());
    }

    String filename01 = "grid_test_0_1";
    DatanodeStorageInfo[] targets2 = replicator.chooseTarget(filename01, 3, dataNodes[0], chosenNodes, true,
            null, BLOCK_SIZE, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY);

    assertEquals(targets1[1].getDatanodeDescriptor(), targets2[0].getDatanodeDescriptor());
    assertFalse(isOnSameRack(targets2[0], targets2[1]));
    assertTrue(isOnSameRack(targets2[1], targets2[2]));


    String filename02 = "grid_test_0_2";
    DatanodeStorageInfo[] targets02 = replicator.chooseTarget(filename02, 3, dataNodes[0], chosenNodes, true,
            null, BLOCK_SIZE, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY);
    assertEquals(targets2[1].getDatanodeDescriptor(), targets02[0].getDatanodeDescriptor());
    assertFalse(isOnSameRack(targets02[0], targets02[1]));
    assertTrue(isOnSameRack(targets02[1], targets02[2]));


    String filename22 = "grid_test_2_2";
    DatanodeStorageInfo[] targets22 = replicator.chooseTarget(filename22, 3, dataNodes[0], chosenNodes, true,
            null, BLOCK_SIZE, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY);
    assertEquals(targets22[0].getDatanodeDescriptor(), targets02[0].getDatanodeDescriptor());
    assertEquals(targets22[1].getDatanodeDescriptor(), targets02[1].getDatanodeDescriptor());


    // TODO  print a many file result, just a file test
    /*
    FileOutputStream fileOutputStream = new FileOutputStream(new File("/home/sparkl/grid_testij_5x5"), true);
    BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        String filename_ij = "grid_testij_" + Integer.toString(i) + "_" + Integer.toString(j);
        DatanodeStorageInfo[] targetsij = replicator.chooseTarget(filename_ij, 3, dataNodes[0], chosenNodes, true,
                null, BLOCK_SIZE, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY);
        for (DatanodeStorageInfo info : targetsij) {
          bufferedWriter.write(info.getDatanodeDescriptor().toString());
          bufferedWriter.write(" ");
        }
        bufferedWriter.write("\n");
      }
    }
    bufferedWriter.close();
    fileOutputStream.close();
    //*/

  }


  /**
   * In this testcase, client is dataNodes[0]. So the 1st replica should be
   * placed on dataNodes[0], the 2nd replica should be placed on
   * different rack and third should be placed on different node
   * of rack chosen for 2nd node.
   * <p>
   * ChooseTarget() will get a filename, by which determine the block placement.
   * <p>
   * <p>
   * The only excpetion is when the <i>numOfReplicas</i> is 2,
   * the 1st is on dataNodes[0] and the 2nd is on a different rack.
   *
   */
  @Test
  public void testChooseTargetSpatialDataBalance() {

    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();

    int[] splitBlockCount = new int[cluster.getNumOfLeaves()];

    for (int i = 0; i < 1; i++) {
      for (int j = 0; j < 150; j++) {
        String filename_ij = "grid_testfile_" + i + "_" + j;
        DatanodeStorageInfo[] targetsij = replicator.chooseTarget(filename_ij, 3, dataNodes[0], chosenNodes, true,
                null, BLOCK_SIZE, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY);

        Assert.assertEquals(targetsij.length, 3);

        String test = targetsij[1].getDatanodeDescriptor().toString();
        int currentSplitNode = Integer.parseInt(targetsij[1].getDatanodeDescriptor().toString().split("\\.")[0]);
        splitBlockCount[currentSplitNode - 1]++;

//        for (DatanodeStorageInfo info : targetsij) {
//          System.out.println(info.getDatanodeDescriptor().toString());
//        }
//        System.out.println("\n");
      }
    }

    for (int num : splitBlockCount) {
    //  System.out.println(num- splitBlockCount[0]);
      Assert.assertTrue(Math.abs(num - splitBlockCount[0]) <2);
    }
  }
}

