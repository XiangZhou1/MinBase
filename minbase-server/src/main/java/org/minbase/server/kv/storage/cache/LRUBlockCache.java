package org.minbase.server.kv.storage.cache;


import org.minbase.server.kv.storage.StoreFile;
import org.minbase.server.kv.storage.block.DataBlock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class LRUBlockCache implements BlockCache {
    private HashMap<CacheBlockKey, Entry<CacheBlockValue>> map;
    private LinkedList<CacheBlockValue> list;
    private volatile long length = 0;
    private long maxCacheSize = 100 * 1024 * 1024;

    public LRUBlockCache(long maxCacheSize) {
        map = new HashMap<>();
        list = new LinkedList<>();//缓存的key,按照存入的顺序存储
        this.maxCacheSize = maxCacheSize;
    }

    public long length() {
        return length;
    }

    @Override
    public synchronized DataBlock get(StoreFile storeFile, int dataBlockIndex) {
        CacheBlockKey cacheBlockKey = new CacheBlockKey(storeFile, dataBlockIndex);
        Entry<CacheBlockValue> blockEntry = map.get(cacheBlockKey);
        if (blockEntry != null) {
            list.remove(blockEntry);
            list.add(blockEntry);
            return blockEntry.getValue().getDataBlock();
        }
        return null;
    }

    @Override
    public synchronized void put(StoreFile storeFile, int dataBlockIndex, DataBlock block) {
        CacheBlockKey cacheBlockKey = new CacheBlockKey(storeFile, dataBlockIndex);
        CacheBlockValue cacheBlockValue = new CacheBlockValue(cacheBlockKey, block);
        evict(cacheBlockKey);

        Entry<CacheBlockValue> blockEntry = new Entry<>(cacheBlockValue);
        map.put(cacheBlockKey, blockEntry);
        list.add(blockEntry);
        length += block.length();

        while (length > maxCacheSize) {
            System.out.println("put and evict");
            evict();
        }
    }

    @Override
    public void evict(StoreFile storeFile, int dataBlockIndex) {
        evict(new CacheBlockKey(storeFile, dataBlockIndex));
    }


    synchronized void evict(CacheBlockKey cacheBlockKey) {
        Entry<CacheBlockValue> blockEntry = map.get(cacheBlockKey);
        if (blockEntry != null) {
            map.remove(cacheBlockKey);
            list.remove(blockEntry);
            length -= blockEntry.getValue().getDataBlock().length();
        }
    }

    @Override
    synchronized public void evict() {
        System.out.println("evict");
        Entry<CacheBlockValue> last = list.last();
        evict(last.getValue().getCacheBlockKey());

    }


    public List<CacheBlockValue> list() {
        List<CacheBlockValue> result = new ArrayList<>();
        Entry<CacheBlockValue> entry = list.first();
        while (entry != null && entry != list.tail) {
            result.add(entry.value);
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

        public void remove(Entry<V> entry) {
            if (entry.next != null) {
                entry.next.prev = entry.prev;
            }
            if (entry.prev != null) {
                entry.prev.next = entry.next;
            }
        }
    }

    public static class Entry<V> {
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
