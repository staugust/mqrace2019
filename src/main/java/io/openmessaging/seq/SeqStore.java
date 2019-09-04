package io.openmessaging.seq;

import io.openmessaging.Message;
import io.openmessaging.btree.Constants;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class SeqStore {

  FileChannel attrF;  //a,a,a,a...
  FileChannel dataF; //data,data,data...
  FileChannel keysF; // entries,key,entries,key...

  FileChannel attrR;
  FileChannel dataR;
  FileChannel keysR;

  long entries = 0;
  long lastTs = MIN_TS;
  final static long MIN_TS = 0x8000000000000000L;
  final static long MAX_TS = 0x7fffffffffffffffL;
  final static int PAGE_SIZE = 4096;

  ByteBuffer attrBuffer = ByteBuffer.allocateDirect(8 * PAGE_SIZE);
  ByteBuffer keyBuffer = ByteBuffer.allocateDirect(16 * PAGE_SIZE);
  ByteBuffer dataBuffer = ByteBuffer.allocateDirect((int) Constants.MsgBodyLen * PAGE_SIZE);

  public SeqStore(int idx) {
    Path treePath = Paths.get(String.format(Constants.Tree_bin, idx));
    Path dataPath = Paths.get(String.format(Constants.Data_bin, idx));
    Path metaPath = Paths.get(String.format(Constants.Meta_bin, idx));
    boolean hasData = Files.exists(treePath);
    try {
      attrF = FileChannel.open(treePath, StandardOpenOption.APPEND, StandardOpenOption.CREATE);//,StandardOpenOption.DSYNC);
      dataF = FileChannel.open(dataPath, StandardOpenOption.APPEND, StandardOpenOption.CREATE);//,StandardOpenOption.DSYNC);
      keysF = FileChannel.open(metaPath, StandardOpenOption.APPEND, StandardOpenOption.CREATE);//,StandardOpenOption.DSYNC);
      attrR = FileChannel.open(treePath, StandardOpenOption.READ);
      dataR = FileChannel.open(dataPath, StandardOpenOption.READ);
      keysR = FileChannel.open(metaPath, StandardOpenOption.READ);
      if (hasData) {
        entries = dataF.size() / Constants.MsgBodyLen;
        ByteBuffer buffer = ByteBuffer.allocate(8);
        keysR.read(buffer, keysR.size() - 8);
        buffer.position(0);
        lastTs = buffer.getLong();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void put(Message msg) {
    if (lastTs == MIN_TS || msg.getT() > lastTs) {
      lastTs = msg.getT();
      keyBuffer.putLong(entries);
      keyBuffer.putLong(lastTs);
      try {
        if (keyBuffer.position() == keyBuffer.capacity()) {
          keyBuffer.position(0);
          int bytes = keysF.write(keyBuffer);
          if (bytes != keyBuffer.capacity()) {
            System.out.printf("write keyBuffer %d bytes.\n", bytes);
            System.exit(-1);
          }
          keyBuffer.clear();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    entries += 1;
    writeMessage(msg);
  }

  public void writeMessage(Message msg) {
    try {
      dataBuffer.put(msg.getBody());
      if (dataBuffer.capacity() == dataBuffer.position()) {
        dataBuffer.position(0);
        int bytes = dataF.write(dataBuffer);
        dataBuffer.clear();
        if (bytes != dataBuffer.capacity()) {
          System.out.printf("write body %d bytes.\n", bytes);
          System.exit(-1);
        }
      }

      attrBuffer.putLong(msg.getA());
      if (attrBuffer.position() == attrBuffer.capacity()) {
        attrBuffer.position(0);
        int bytes = attrF.write(attrBuffer);
        attrBuffer.clear();
        if (bytes != attrBuffer.capacity()) {
          System.out.printf("write attr %d bytes.\n", bytes);
          System.exit(-1);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
    List<Message> res = new LinkedList<>();
    ByteBuffer contentBuffer = ByteBuffer.allocate(16);
    long keyIdx = 0;
    long prevKey = 0;
    long nextKey = 0;
    long prevIdx = 0;
    long nextIdx = 0;
    try {
      contentBuffer.clear();
      keysR.read(contentBuffer, keyIdx * 16);
      keyIdx += 1;
      prevIdx = contentBuffer.getLong(0);
      prevKey = contentBuffer.getLong(8);
      long totalKeys = keysF.size() / 16;
      for (; keyIdx <= totalKeys; keyIdx++) {
        if (keyIdx != totalKeys) {
          contentBuffer.clear();
          keysR.read(contentBuffer, keyIdx * 16);
          nextIdx = contentBuffer.getLong(0);
          nextKey = contentBuffer.getLong(8);
        } else {
          nextIdx = entries;
          nextKey = MAX_TS;
        }
        if (prevKey >= tMin && prevKey <= tMax) {
          for (long offset = prevIdx; offset < nextIdx; offset++) {
            Message msg = readMsg(prevKey, offset, aMin, aMax);
            if (msg != null) {
              res.add(msg);
            }
          }
        }
        prevIdx = nextIdx;
        prevKey = nextKey;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return res;
  }

  public Message readMsg(long t, long idx, long aMin, long aMax) {
    ByteBuffer attrBuffer = ByteBuffer.allocate(8);
    byte[] bytes = new byte[(int) Constants.MsgBodyLen];
    long a = MIN_TS;
    try {
      attrR.read(attrBuffer, idx * 8);
      dataR.read(ByteBuffer.wrap(bytes), idx * Constants.MsgBodyLen);
      a = attrBuffer.getLong(0);
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (a < aMin || a > aMax) {
      return null;
    }
    return new Message(attrBuffer.getLong(0), t, bytes);
  }

  public List<Long> getAvgValue(long aMin, long aMax, long tMin, long tMax) {
    long sumA = 0;
    long count = 0;
    try {
      long h = findTMin(tMin);
      long t = findTMax(tMax);
      ByteBuffer attrBuffer = ByteBuffer.allocate(8);
      for (long offset = h; offset < t; offset++) {
        attrBuffer.clear();
        attrR.read(attrBuffer, offset * 8);
        long a = attrBuffer.getLong(0);
        if (a >= aMin && a <= aMax) {
          sumA += a;
          count += 1;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    List<Long> lst = new ArrayList<>();
    lst.add(count);
    lst.add(sumA);
    return lst;
  }

  long findTMin(long tmin) {
    try {
      long h = 0;
      long t = keysR.size() / 16 - 1;
      long mid = (h + t) / 2;
      long hKey = 0;
      long tKey = 0;
      long midKey = 0;
      ByteBuffer buffer = ByteBuffer.allocate(16);

      keysR.read(buffer, h);
      hKey = buffer.getLong(8);
      if (hKey >= tmin) {
        return 0;
      }
      buffer.clear();
      keysR.read(buffer, 16 * t);
      tKey = buffer.getLong(8);
      if (tKey < tmin) {
        return entries;
      }else if(tKey == tmin){
        return buffer.getLong(0);
      }

      while (t - h > 1) {
        mid = (h + t) / 2;
        buffer.clear();
        keysR.read(buffer, mid * 16);
        midKey = buffer.getLong(8);
        if(midKey == tmin){
          return buffer.getLong(0);
        }else if(midKey < tmin){
          h = mid;
        }else{
          t = mid;
        }
      }
      buffer.clear();
      keysR.read(buffer, t * 16);
      return buffer.getLong(0);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return 0;
  }


  long findTMax(long tmax) {
    try {
      long h = 0;
      long t = keysR.size() / 16 - 1;
      long mid = (h + t) / 2;
      long hKey = 0;
      long tKey = 0;
      long midKey = 0;
      ByteBuffer buffer = ByteBuffer.allocate(16);

      keysR.read(buffer, h);
      hKey = buffer.getLong(8);
      if (hKey > tmax) {
        return 0;
      } else if (hKey == tmax) {
        buffer.clear();
        keysR.read(buffer, (h+1) * 16);
        return buffer.getLong(0);
      }
      buffer.clear();
      keysR.read(buffer, 16 * t);
      tKey = buffer.getLong(8);
      if (tKey <= tmax) {
        return entries;
      }

      while (t - h > 1) {
        mid = (h + t) / 2;
        buffer.clear();
        keysR.read(buffer, mid * 16);
        midKey = buffer.getLong(8);
        if(midKey == tmax){
          buffer.clear();
          keysR.read(buffer, (mid+1) * 16);
          return buffer.getLong(0);
        }else if(midKey < tmax){
          h = mid;
        }else{
          t = mid;
        }
      }
      buffer.clear();
      keysR.read(buffer, t * 16);
      return buffer.getLong(0);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return 0;
  }

  void force() {
    try {
      int len = keyBuffer.position();
      byte[] keyBytes = new byte[len];
      keyBuffer.position(0);
      keyBuffer.get(keyBytes, 0, len);
      int bytes = keysF.write(ByteBuffer.wrap(keyBytes));
      if (bytes != len) {
        System.out.println("write sub-keybuffer  " + bytes + " which should be " + len);
        System.exit(-1);
      }
      len = dataBuffer.position();
      byte[] dataBytes = new byte[len];
      dataBuffer.position(0);
      dataBuffer.get(dataBytes, 0, len);
      bytes = dataF.write(ByteBuffer.wrap(dataBytes));
      if (bytes != len) {
        System.out.println("write sub-databuffer  " + bytes + " which should be " + len);
        System.exit(-1);
      }

      len = attrBuffer.position();
      byte[] attrBytes = new byte[len];
      attrBuffer.position(0);
      attrBuffer.get(attrBytes, 0, len);
      bytes = attrF.write(ByteBuffer.wrap(attrBytes));
      if (bytes != len) {
        System.out.println("write sub-attrbuffer  " + bytes + " which should be " + len);
        System.exit(-1);
      }
      if(attrR.size() != attrF.size()){
        System.out.println("attr read write not the same size " + attrR.size() + " : " + attrF.size());
      }
      if(dataR.size() != dataF.size()){
        System.out.println("attr read write not the same size " + dataR.size() + " : " + dataF.size());
      }
      if(keysR.size() != keysF.size()){
        System.out.println("attr read write not the same size " + keysR.size() + " : " + keysF.size());
      }
      attrR.force(true);
      dataR.force(true);
      keysR.force(true);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
