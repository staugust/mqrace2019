package io.openmessaging;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MapFile {

  public MapFile(String path, long size) {
    try {
      RandomAccessFile aFile = new RandomAccessFile(path, "rw");
      FileChannel fc = aFile.getChannel();
      MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_WRITE, 4096, size);
      buffer.put("abcdef".getBytes(), 0, 4);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    MapFile mp = new MapFile("D:/tmp/zzz.dat", 4096);
  }
}
