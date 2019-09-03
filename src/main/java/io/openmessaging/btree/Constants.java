package io.openmessaging.btree;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Constants {
  public static final int TREE_ROOT_LEAF = 1;// children are TREE_DATANODE
  public static final int TREE_ROOT = 2;  //children are TREE_NODE
  public static final int TREE_NODE = 3; //common TREE_NODE
  //all nodes above have same size

  public static final int TREE_LEAF = 4;  // leaf , contains keys, offsets, As, capacity, branches
  public static final int TREE_DATA = 5; //  []A , []offsets , next block, prev block, capacity, branches
  // node with those two type have same size
  public static final int TREE_DATA_OVERFLOW = 6;

  public static final int TreeBranches = 61; // makes each node 1024 bytes. (1024 - 8*6) / (8+8)
  public static final int TreeNodeSize = 1024; //

  //TODO change data_dir
  public static final String DATA_DIR = "/alidata1/race2019/data/";
  //public static final String DATA_DIR = "D:\\tmp\\alidata\\";

  public static final String Tree_bin = (DATA_DIR + "%d" + ".attr");
  public static final String Data_bin = (DATA_DIR + "%d" + ".dat");
  public static final String Meta_bin = (DATA_DIR + "%d" + ".keys");

  public static final long FileBlockSize = 0x40000000L; //increate 1GB

  //TODO modify to 34
  public static final long MsgBodyLen = 34L;
}
