package io.openmessaging.seq;

import io.openmessaging.Message;
import io.openmessaging.btree.Constants;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SeqStoreManager {

  public static Map<Integer, SeqStore> Stores = new HashMap<>();
  public static FileChannel cntF;
  public static Integer Index = 0;
  public ThreadLocal<SeqStore> currentStore = ThreadLocal.withInitial(SeqStoreManager::GetStore);

  AtomicBoolean shouldForce = new AtomicBoolean(true);
  public SeqStoreManager() {
    try {
      int storeCount = GetStoreCount();
      for (int idx = 0; idx < storeCount; idx++) {
        Stores.put(idx, new SeqStore(idx));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public synchronized static SeqStore GetStore() {
    int idx = 0;
    synchronized (Index) {
      idx = Index;
      Index += 1;
      ByteBuffer buffer = ByteBuffer.allocate(4);
      buffer.putInt(idx);
      if (cntF == null) {
        try {
          cntF = FileChannel.open(Paths.get(Constants.DATA_DIR + "index.count"), StandardOpenOption.READ, StandardOpenOption.WRITE,
              StandardOpenOption.DSYNC, StandardOpenOption.CREATE, StandardOpenOption.SPARSE);
          cntF.truncate(8);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      try {
        buffer.position(0);
        int writeBytes = cntF.write(buffer, 0);
        System.out.println("write into cntF " + writeBytes + " value " + buffer.getInt(0));
        cntF.force(true);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    Stores.put(idx, new SeqStore(idx));
    return Stores.get(idx);
  }

  public static int GetStoreCount() {
    int count = 0;
    try {
      FileChannel fc = FileChannel.open(Paths.get(Constants.DATA_DIR + "index.count"), StandardOpenOption.READ, StandardOpenOption.WRITE,
          StandardOpenOption.DSYNC, StandardOpenOption.CREATE, StandardOpenOption.SPARSE);
      fc.truncate(8);
      ByteBuffer buffer = ByteBuffer.allocate(4);
      fc.read(buffer);
      count = buffer.getInt(0);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return count;
  }

  public void put(Message message) {
    shouldForce.set(true);
    currentStore.get().put(message);
  }

  public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
    forceSave();
    LinkedList<Message> res = new LinkedList<>();
    Stores.values().forEach(store -> {
      res.addAll(store.getMessage(aMin, aMax, tMin, tMax));
    });
    res.sort((x, y) -> {
      if(x.getT() < y.getT()){
        return -1;
      }else if(x.getT() == y.getT()){
        return 0;
      }else{
        return 1;
      }
    });
    return res;
  }

  public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
    forceSave();
    AtomicLong count = new AtomicLong(0);
    AtomicLong sumA = new AtomicLong(0);
    Stores.values().forEach(store -> {
      List<Long> lst = store.getAvgValue(aMin, aMax, tMin, tMax);
      count.getAndAdd(lst.get(0));
      sumA.getAndAdd(lst.get(1));
    });
    return (long) (sumA.get() / count.get());
  }

  public void forceSave() {
    if(shouldForce.compareAndSet(true, false)){
      synchronized (Stores) {
        Stores.values().forEach(store -> {
          store.force();
        });
      }
    }
  }
}
