package io.openmessaging.btree;

import io.openmessaging.Message;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

public class BTree {
  TreeNode root;
  TreeNode leftLeaf;
  TreeNode rightLeaf;
  AtomicLong dataPos;
  AtomicLong nodePos;
  long rootOffset;
  long leftLeafOffset;
  long rightLeafOffset;

  FileChannel treeF;
  FileChannel dataF;

  public BTree(int idx) {
    Path treePath = Paths.get(String.format(Constants.Tree_bin, idx));
    Path dataPath = Paths.get(String.format(Constants.Data_bin, idx));
    try {
      treeF = FileChannel.open(treePath, StandardOpenOption.READ, StandardOpenOption.WRITE,
          StandardOpenOption.DSYNC, StandardOpenOption.SPARSE, StandardOpenOption.CREATE);
      dataF = FileChannel.open(dataPath, StandardOpenOption.READ, StandardOpenOption.WRITE,
          StandardOpenOption.DSYNC, StandardOpenOption.SPARSE, StandardOpenOption.CREATE);
      if(treeF.size() < Constants.FileBlockSize){
        treeF.truncate(Constants.FileBlockSize);
      }
      if(dataF.size() < Constants.FileBlockSize){
        dataF.truncate(Constants.FileBlockSize);
      }

    }catch (Exception e){
      e.printStackTrace();
    }
  }

  public void put(Message message) {
    if (root.nodeType.get() == Constants.TREE_ROOT_LEAF) {
      // check and add
      long idx = root.capacity.getAndIncrement();
      if (idx >= Constants.TreeBranches) {
        root.capacity.getAndDecrement();
      } else {
      }
    }else{
      // from the right most leaf node.

    }
  }

  public long writeData(Message msg) {
    long pos = nodePos.addAndGet(34);
    return pos;
  }

}
