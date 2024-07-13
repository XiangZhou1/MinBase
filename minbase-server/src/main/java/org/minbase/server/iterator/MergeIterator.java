package org.minbase.server.iterator;



import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.common.utils.ByteUtils;
import org.minbase.server.utils.KeyUtils;

import java.util.List;
import java.util.PriorityQueue;

public class MergeIterator implements KeyIterator {
    private PriorityQueue<KeyIterator> queue;

    public MergeIterator(List<KeyIterator> iterators) {
        this.queue = new PriorityQueue<>(KeyUtils.KEY_ITERATOR_COMPARATOR);

        for (KeyIterator iterator : iterators) {
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
    public void nextKey() {
        if (isValid()) {
            KeyIterator poll = queue.poll();

            // 将新poll出来的迭代器在加进去
            poll.nextKey();
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
            nextKey();
        }
    }

    // 跳到下一个userKey
    @Override
    public void nextUserKey() {
        Key key = key();

        while (!queue.isEmpty()) {
            KeyIterator firstEntry = queue.peek();
            if (ByteUtils.byteEqual(firstEntry.key().getUserKey(), key.getUserKey())) {
                queue.poll();
                // 将新poll出来的迭代器在加进去
                firstEntry.nextUserKey();
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
