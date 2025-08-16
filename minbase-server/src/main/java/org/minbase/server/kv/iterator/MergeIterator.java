package org.minbase.server.kv.iterator;


import org.minbase.server.kv.Key;
import org.minbase.server.kv.KeyValue;
import org.minbase.server.kv.utils.KeyValueUtil;

import java.util.List;
import java.util.PriorityQueue;

public class MergeIterator extends AbstractKeyValueIterator {
    private PriorityQueue<KeyValueIterator> queue;


    public MergeIterator(List<KeyValueIterator> iterators) {
        this(iterators, null, null);
    }

    public MergeIterator(List<KeyValueIterator> iterators, Key startKey, Key endKey) {
        super(startKey, endKey);
        this.queue = new PriorityQueue<>(KeyValueUtil.KEY_ITERATOR_COMPARATOR);
        for (KeyValueIterator iterator : iterators) {
            if (iterator.hasNext()) {
                KeyValue next = iterator.next();
                if (next != null) {
                    queue.add(iterator);
                } else {
                    iterator.close();
                }
            } else {
                iterator.close();
            }
        }

        if (startKey != null) {
            seek(startKey);
        }
    }

    @Override
    protected boolean hasNextInternal() {
        return !queue.isEmpty();
    }

    // 跳到下一个Key
    @Override
    public KeyValue nextInternal() {
        if (!queue.isEmpty()) {
            KeyValueIterator firstEntry = queue.peek();
            queue.poll();
            KeyValue keyValue = firstEntry.value();
            // 将新poll出来的迭代器在加进去
            if (firstEntry.hasNext() && firstEntry.next() != null) {
                queue.add(firstEntry);
            } else {
                firstEntry.close();
            }
            return keyValue;
        } else {
            return null;
        }
    }

    @Override
    public void close() {
        while (!queue.isEmpty()) {
            KeyValueIterator poll = queue.poll();
            poll.close();
        }
    }
}
