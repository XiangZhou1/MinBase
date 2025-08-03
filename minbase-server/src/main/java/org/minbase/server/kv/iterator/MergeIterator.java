package org.minbase.server.kv.iterator;


import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.kv.utils.KeyValueUtil;

import java.util.List;
import java.util.PriorityQueue;

public class MergeIterator implements KeyValueIterator {
    private PriorityQueue<KeyValueIterator> queue;
    private KeyValue current;
    public MergeIterator(List<KeyValueIterator> iterators) {
        this.queue = new PriorityQueue<>(KeyValueUtil.KEY_ITERATOR_COMPARATOR);
        for (KeyValueIterator iterator : iterators) {
            if (iterator.hasNext()) {
                iterator.next();
                queue.add(iterator);
            }
        }
    }

    @Override
    public KeyValue value() {
        if (current == null) {
            return null;
        }
        return current;
    }

    @Override
    public Key key() {
        if (current == null) {
            return null;
        }
        return current.getKey();
    }

    @Override
    public boolean hasNext() {
        if (queue == null || queue.isEmpty()) {
            return false;
        }
        return true;
    }

    @Override
    public void nextInnerKey() {
//        if (hasNext()) {
//            KeyValueIterator poll = queue.poll();
//
//            // 将新poll出来的迭代器在加进去
//            poll.nextInnerKey();
//            if (poll.hasNext()) {
//                queue.add(poll);
//            } else {
//                poll.close();
//            }
//        }
    }


    @Override
    public void seek(Key key) {
        current = next();
        while (current != null && key().compareTo(key) < 0 && hasNext()) {
            next();
        }
    }

    // 跳到下一个Key
    @Override
    public KeyValue next() {
        if (!queue.isEmpty()) {
            KeyValueIterator firstEntry = queue.peek();
            queue.poll();
            current = firstEntry.value();
            // 将新poll出来的迭代器在加进去
            if (firstEntry.hasNext()) {
                firstEntry.next();
                queue.add(firstEntry);
            } else {
                firstEntry.close();
            }
            return current;
        } else {
            return null;
        }
    }
}
