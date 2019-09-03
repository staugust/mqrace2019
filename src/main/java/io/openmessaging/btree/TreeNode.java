package io.openmessaging.btree;

import com.sun.corba.se.impl.orbutil.closure.Constant;
import com.sun.xml.internal.bind.v2.runtime.reflect.opt.Const;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class TreeNode {

  public TreeNode(){
    this.keys = new long[Constants.TreeBranches];
    this.offsets = new long[Constants.TreeBranches];
    this.children = new WeakReference[Constants.TreeBranches];
  }
  public TreeNode(int nodetype, long offset, long parent) {
    this.keys = new long[Constants.TreeBranches];
    this.offsets = new long[Constants.TreeBranches];
    this.self = new AtomicLong(offset);
    this.parent = new AtomicLong(parent);
    this.nodeType = new AtomicLong(nodetype);
    this.capacity = new AtomicLong(0);
  }

  public TreeNode(long offset, FileChannel fc){
    ByteBuffer buffer = ByteBuffer.allocate(Constants.TreeNodeSize);
    try {
      fc.read(buffer, offset);
      buffer.position(0);
      this.nodeType = new AtomicLong(buffer.getLong());
      this.capacity = new AtomicLong(buffer.getLong());
      this.self = new AtomicLong(buffer.getLong());
      this.parent = new AtomicLong(buffer.getLong());
      this.prev = new AtomicLong(buffer.getLong());
      this.next = new AtomicLong(buffer.getLong());
      this.keys = new long[Constants.TreeBranches];
      for(int i = 0; i < capacity.get(); i++){
        this.keys[i] = buffer.getLong();
      }
      this.offsets = new long[Constants.TreeBranches];
      buffer.position(8 * (6 + Constants.TreeBranches));
      for(int i = 0; i < capacity.get(); i++){
        this.offsets[i] = buffer.getLong();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void load(FileChannel fc) {

  }

  public AtomicLong nodeType;
  public AtomicLong capacity;
  public AtomicLong self;
  public AtomicLong parent;
  public AtomicLong prev;
  public AtomicLong next;
  public long[] keys; // in TreeLeaf, this is the attribute
  public long[] offsets; // len(offsets) == len(keys) + 1, in TreeLeaf, this is the offset of data part.

  public WeakReference<TreeNode> parentNode = null;
  public WeakReference<TreeNode>[] children ;


  public void write(){
    ByteBuffer buffer = ByteBuffer.allocate(Constants.TreeNodeSize);
    buffer.putLong(nodeType.get());
    buffer.putLong(capacity.get());
    buffer.putLong(self.get());
    buffer.putLong(parent.get());
    buffer.putLong(prev.get());
    buffer.putLong(next.get());
    for(int i = 0; i < capacity.get(); i++){
      buffer.putLong(keys[i]);
    }
    buffer.position(8 * (6 + Constants.TreeBranches));
    for(int i= 0; i < capacity.get(); i++){
      buffer.putLong(offsets[i]);
    }
  }

  public  long getNodeType() {
    return nodeType.get();
  }

  public void setNodeType(long type) {
    nodeType.set(type);
  }

}
