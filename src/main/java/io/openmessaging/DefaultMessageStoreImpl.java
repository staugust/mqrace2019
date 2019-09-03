package io.openmessaging;

import io.openmessaging.bplus.bptree.BPlusTree;
import io.openmessaging.bplus.util.InvalidBTreeStateException;
import io.openmessaging.seq.SeqStore;
import io.openmessaging.seq.SeqStoreManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

    SeqStoreManager manager = null;

    public DefaultMessageStoreImpl(){
        manager = new SeqStoreManager();
    }

    @Override
    public synchronized void put(Message message) {
        manager.put(message);
    }


    @Override
    public synchronized List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        return manager.getMessage(aMin, aMax, tMin, tMax);
    }


    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
       return manager.getAvgValue(aMin, aMax, tMin, tMax);

    }

}
