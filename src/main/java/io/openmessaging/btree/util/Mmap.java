package io.openmessaging.btree.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Scanner;

public class Mmap {
  final static int IndexBinSize = 0x1000;
  public Mmap(Path path) {
    try {
      fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE,
          StandardOpenOption.DSYNC, StandardOpenOption.SPARSE, StandardOpenOption.CREATE);
      fc.truncate(IndexBinSize);
      byte[] bytes = "Augusto".getBytes();
      ByteBuffer buf =  ByteBuffer.wrap("Augusto".getBytes());
      fc.position(IndexBinSize / 2);
      buf.position(0);
      fc.write(buf);
      fc.position(IndexBinSize / 2);
      fc.write(ByteBuffer.wrap("zidayezhenniub".getBytes()));
      fc.position(0);
      fc.write(ByteBuffer.wrap("nidayeshizhongshinidaye".getBytes()));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  FileChannel fc;

  public void print() throws IOException {
    long sz = fc.size();
    fc.position(0);
    System.out.println("File size : " + sz);
    ByteBuffer buf = ByteBuffer.allocate(1024 * 1024);
    for(long idx = 0; idx < sz; idx+=1024 * 1024){
      buf.clear();
      buf.position(0);
      fc.read(buf);
      buf.position(0);
      if (idx > 535822336) {
        for (int i = 0 ; i< buf.limit(); i+= 1024) {

          System.out.printf("" + idx + " " + i + "-> 0x");
          buf.position(i);
          try {
            byte c = buf.get();
            System.out.printf("%c", (char) c);
          } catch (Exception e) {
            e.printStackTrace();
            break;
          }
          System.out.println();
        }
      }
    }
    fc.close();
  }

  public void write(int pos, String str) {
    try {
      System.out.println(""+pos + "/"+ str.length() + " -> " + str);
      fc.position(pos);
      fc.write(ByteBuffer.wrap(str.getBytes()));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void printStr(int pos, int len) {
    try {
      ByteBuffer buf = ByteBuffer.allocate(len);
      fc.read(buf, pos);
      buf.position(0);
      System.out.printf("%d/%d", pos, len);
      for(int i = 0; i < buf.capacity(); i++){
        byte c = buf.get();
        System.out.printf("%c", c);
      }
      System.out.println();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    Mmap mmap = new Mmap(Paths.get("D:\\tmp\\test.dat"));
    int pos = 0;
    String str = "";
    int len = 0;
    Scanner scanner = new Scanner(System.in);
    while( (pos=scanner.nextInt()) > 0){
      str = scanner.next();
      mmap.write(pos, str);
    }
    pos = 0;
    while((pos = scanner.nextInt()) > 0){
      len = scanner.nextInt();
      mmap.printStr(pos, len);
    }
  }
}
