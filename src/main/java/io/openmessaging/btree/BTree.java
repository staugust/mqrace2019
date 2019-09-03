package io.openmessaging.btree;

import io.openmessaging.Message;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

public class BTree {
  TreeNode root;
  TreeNode leftLeaf;
  TreeNode rightLeaf;
  long rootOffset;
  long leftLeafOffset;
  long rightLeafOffset;

  FileChannel treeF;
  FileChannel dataF;
  AtomicLong dataPos;
  AtomicLong nodePos;
  AtomicLong dataFileSize = new AtomicLong(0);
  AtomicLong treeFileSize = new AtomicLong(0);

  public BTree(int idx) {
    Path treePath = Paths.get(String.format(Constants.Tree_bin, idx));
    Path dataPath = Paths.get(String.format(Constants.Data_bin, idx));
    try {
      treeF = FileChannel.open(treePath, StandardOpenOption.READ, StandardOpenOption.WRITE,
          StandardOpenOption.DSYNC, StandardOpenOption.SPARSE, StandardOpenOption.CREATE);
      dataF = FileChannel.open(dataPath, StandardOpenOption.READ, StandardOpenOption.WRITE,
          StandardOpenOption.DSYNC, StandardOpenOption.SPARSE, StandardOpenOption.CREATE);
      if (treeF.size() < Constants.FileBlockSize) {
        treeF.truncate(Constants.FileBlockSize);
      }
      if (dataF.size() < Constants.FileBlockSize) {
        dataF.truncate(Constants.FileBlockSize);
      }

    } catch (Exception e) {
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
    } else {
      // from the right most leaf node.

    }
  }

  public long writeData(Message msg) {

    long pos = nodePos.addAndGet(Constants.MsgBodyLen);
    return pos;
  }

  public Message readMessage(long offset, long t, long a) {
    byte[] bytes = new byte[(int) Constants.MsgBodyLen];
    try {
      dataF.read(ByteBuffer.wrap(bytes), offset);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return new Message(a, t, bytes);
  }

  static class Test implements Runnable{
    Integer x;
    public Test(Integer i){
      this.x = i;
    }
    @Override
    public void run() {
      synchronized (x) {
        System.out.println(x);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        System.out.println(x);
      }
    }
  }

  public static void main(String[] args) {
    Integer x = 0;
    Thread th1 = new Thread(new Test(x));
    th1.start();
    try {
      Thread.sleep(500);
    } catch (Exception e) {
      e.printStackTrace();
    }
    x = new Integer(4);
    System.out.println("main thread " + x);
    try {
      th1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }
}
