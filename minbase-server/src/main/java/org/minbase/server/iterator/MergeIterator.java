package org.minbase.server.iterator;



import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.common.utils.ByteUtil;
import org.minbase.server.utils.KeyValueUtil;

import java.util.List;
import java.util.PriorityQueue;

public class MergeIterator implements KeyValueIterator {
    private PriorityQueue<KeyValueIterator> queue;

    public MergeIterator(List<KeyValueIterator> iterators) {
        this.queue = new PriorityQueue<>(KeyValueUtil.KEY_ITERATOR_COMPARATOR);

        for (KeyValueIterator iterator : iterators) {
            if (iterator.isValid()) {
                queue.add(iterator);
            }
        }
    }

    @Override
    public KeyValue value() {
        return queue.peek().value();
    }

    @Override
    public Key key() {
        return queue.peek().key();
    }

    @Override
    public boolean isValid() {
        if (queue == null || queue.isEmpty()) {
            return false;
        }
        return true;
    }

    @Override
    public void nextInnerKey() {
        if (isValid()) {
            KeyValueIterator poll = queue.poll();

            // 将新poll出来的迭代器在加进去
            poll.nextInnerKey();
            if (poll.isValid()) {
                queue.add(poll);
            } else {
                poll.close();
            }
        }
    }


    @Override
    public void seek(Key key) {
        while (isValid() && key().compareTo(key) < 0) {
            nextInnerKey();
        }
    }

    // 跳到下一个userKey
    @Override
    public void next() {
        Key key = key();

        while (!queue.isEmpty()) {
            KeyValueIterator firstEntry = queue.peek();
            if (ByteUtil.byteEqual(firstEntry.key().getUserKey(), key.getUserKey())) {
                queue.poll();
                // 将新poll出来的迭代器在加进去
                firstEntry.next();
                if (firstEntry.isValid()) {
                    queue.add(firstEntry);
                } else {
                    firstEntry.close();
                }
            } else {
                break;
            }
        }
    }
}
