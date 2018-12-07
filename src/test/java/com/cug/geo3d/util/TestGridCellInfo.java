package com.cug.geo3d.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestGridCellInfo {

  @Test
  public void testGetGridIndexFromFilename(){
    GridCellInfo pos = new GridCellInfo();
    assertEquals(GridCellInfo.getGridIndexFromFilename("test/grid_test.dat_2_3", pos), true);
    assertEquals(pos.rowId, 2);
    assertEquals(pos.colId, 3);
    assertEquals(pos.filepath, "test/");
  }

}
