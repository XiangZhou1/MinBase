package org.minbase.server.mem;



import org.minbase.server.conf.Config;
import org.minbase.server.constant.Constants;
import org.minbase.server.iterator.MemTableIterator;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.op.Value;
import org.minbase.server.utils.ByteUtils;
import org.minbase.server.utils.Utils;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;

public class MemTable {
    private static long MAX_MEMTABLE_SIZE = Utils.parseUnit(Config.get(Constants.KEY_MAX_MEMTABLE_SIZE));
    private ConcurrentSkipListMap<byte[], ConcurrentSkipListMap<Key, KeyValue>> map;
    private long dataLength = 0;

    public MemTable() {
        this.map = new ConcurrentSkipListMap<>(ByteUtils.BYTE_ORDER_COMPARATOR);
    }

    public void put(Key key, Value value) {
        final ConcurrentSkipListMap<Key, KeyValue> userKeyMap = map.computeIfAbsent(key.getUserKey(), new Function<byte[], ConcurrentSkipListMap<Key, KeyValue>>() {
            @Override
            public ConcurrentSkipListMap<Key, KeyValue> apply(byte[] bytes) {
                return new ConcurrentSkipListMap<>();
            }
        });

        userKeyMap.put(key, new KeyValue(key, value));
        dataLength += key.length() + value.length();
    }

    /**
     * 根据userKey查找对应的KeyValue值,
     * 返回的是version最新的KeyValue值(即最近操作的值)
     *  1 如果key.version是LATEST_VERSION, 则返回当前最新值,
     *  2 否则返回version < 指定version的值(当前能看到的值)
     */
    public KeyValue get(Key key) {
        ConcurrentSkipListMap<Key, KeyValue> userKeyMap = this.map.get(key.getUserKey());
        if (Objects.isNull(userKeyMap)) {
            return null;
        } else {
            if (key.isLatestVersion()) {
                return userKeyMap.firstEntry().getValue();
            } else {
                Iterator<Map.Entry<Key, KeyValue>> iterator = userKeyMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Key, KeyValue> entry = iterator.next();
                    if (entry.getKey().compareTo(key) >= 0) {
                        return entry.getValue();
                    }
                }
            }
        }
        return null;
    }

    /**
     * 根据userKey查找对应的KeyValue值,
     * 返回的是version最新的KeyValue值(即最近操作的值)
     * @param userKey
     */
    public KeyValue get(byte[] userKey) {
        return get(Key.latestKey(userKey));
    }

    public Set<Map.Entry<byte[], ConcurrentSkipListMap<Key, KeyValue>>> getEntrySet() {
        Set<Map.Entry<byte[], ConcurrentSkipListMap<Key, KeyValue>>> entries = map.entrySet();
        return entries;
    }

    public MemTableIterator iterator(Key startKey, Key endKey) {
        return new MemTableIterator(this, startKey, endKey);
    }

    public MemTableIterator iterator() {
        return new MemTableIterator(this, null, null);
    }

    public long getLength() {
        return dataLength;
    }

    public boolean shouldFreeze() {
        return dataLength >= MAX_MEMTABLE_SIZE;
    }
}
