package org.minbase.server.storage.cache;



import org.minbase.server.conf.Config;
import org.minbase.server.constant.Constants;
import org.minbase.server.storage.block.DataBlock;
import org.minbase.common.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class LRUBlockCache implements BlockCache {
    public static BlockCache BlockCache = new LRUBlockCache();
    private HashMap<String, Entry<DataBlock>> map;
    private LinkedList<DataBlock> list;
    private volatile long length = 0;
    private static int MAX_CACHE_SIZE = (int) Utils.parseUnit(Config.get(Constants.KEY_MAX_CACHE_SIZE));


    public LRUBlockCache() {
        map = new HashMap<>();
        list = new LinkedList<>();//缓存的key,按照存入的顺序存储
    }

    public long length(){
        return length;
    }

    @Override
    synchronized public DataBlock get(String blockId) {
        Entry<DataBlock> blockEntry = map.get(blockId);
        if (blockEntry != null) {
            list.remove(blockEntry);
            list.add(blockEntry);
            return blockEntry.getValue();
        }
        return null;
    }

    @Override
    synchronized public void put(String blockId, DataBlock block) {
        evict(blockId);

        Entry<DataBlock> blockEntry = new Entry<>(block);
        map.put(blockId, blockEntry);
        list.add(blockEntry);
        length += block.length();

        while (length > MAX_CACHE_SIZE) {
            System.out.println("put and evict");
            evict();
        }
    }

    @Override
    synchronized public void evict(String blockId) {
        Entry<DataBlock> blockEntry = map.get(blockId);
        if (blockEntry != null) {
            map.remove(blockId);
            list.remove(blockEntry);
            length -= blockEntry.getValue().length();
        }
    }

    @Override
    synchronized public void evict() {
        System.out.println("evict");
        Entry<DataBlock> last = list.last();
        if (last != null) {
            System.out.println("evit " + last.getValue().getBlockId());
            evict(last.getValue().getBlockId());
        }else{
            System.out.println("null");
        }
    }


    public List<String> list() {
        List<String> result = new ArrayList<>();
        Entry<DataBlock> entry = list.first();
        while (entry != null && entry != list.tail) {
            result.add(entry.value.getBlockId());
            entry = entry.next;
        }
        return result;
    }

    public static class LinkedList<V> {
        private Entry<V> head;
        private Entry<V> tail;

        public LinkedList() {
            head = new Entry<>();
            tail = new Entry<>();
            head.next = tail;
            tail.prev = head;
        }

        public void add(Entry<V> entry) {
            head.addNext(entry);
        }

        public void addLast(Entry<V> entry) {
            tail.addPrev(entry);
        }

        public Entry<V> first() {
            if (head.next != tail) {
                return head.next;
            }
            return null;
        }

        public Entry<V> last() {
            if (tail.prev != head) {
                return tail.prev;
            }
            return null;
        }

        public void remove(Entry<V> entry){
            if (entry.next != null) {
                entry.next.prev = entry.prev;
            }
            if (entry.prev != null) {
                entry.prev.next = entry.next;
            }
        }
    }

    public static class Entry<V>{
        V value;
        Entry<V> prev;
        Entry<V> next;

        public Entry() {
        }

        public Entry(V value) {
            this.value = value;
        }

        public void addNext(Entry<V> entry) {
            this.next.prev = entry;
            entry.next = this.next;

            this.next = entry;
            entry.prev = this;
        }

        public void addPrev(Entry<V> entry) {
            this.prev.next = entry;
            entry.prev = this.prev;

            this.prev = entry;
            entry.next = this;
        }

        public V getValue() {
            return value;
        }

        public void setValue(V value) {
            this.value = value;
        }

        public Entry<V> getNext() {
            return next;
        }

        public void setNext(Entry<V> next) {
            this.next = next;
        }

    }
}
