package io.openmessaging.btree.util;

import io.openmessaging.btree.Constants;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.Lock;

public class FileHelper {
  FileChannel treeFc = null;
  long treeSz = 0;
  FileChannel dataFc = null;
  long dataSz = 0;
  private FileHelper(){
    try {


    }catch (Exception e){

    }
  }

  public void writeIndex(long pos, ByteBuffer buffer) {
    if(pos + buffer.capacity() > treeSz) {
      synchronized (treeFc){
        if(pos + buffer.capacity() > treeSz){
          treeSz += Constants.FileBlockSize;
          try {
            treeFc.truncate(treeSz);
          }catch (Exception e){
            e.printStackTrace();
          }
        }
      }
    }
    try {
      treeFc.write(buffer, pos);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void writeData(long pos, ByteBuffer buffer) {
    if(pos + buffer.capacity() > dataSz) {
      synchronized (dataFc){
        if(pos + buffer.capacity() > dataSz){
          dataSz += Constants.FileBlockSize;
          try {
            dataFc.truncate(dataSz);
          }catch (Exception e){
            e.printStackTrace();
          }
        }
      }
    }
    try {
      dataFc.write(buffer, pos);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public FileChannel getTreeFc() {
    return treeFc;
  }
  public FileChannel getDataFc(){
    return dataFc;
  }

  private static FileHelper instance = new FileHelper();
  public static FileHelper Instance() {
    return instance;
  }





}
