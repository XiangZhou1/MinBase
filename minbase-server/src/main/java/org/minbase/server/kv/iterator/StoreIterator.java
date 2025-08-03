package org.minbase.server.kv.iterator;

import org.minbase.server.kv.storage.version.EditVersion;

import java.util.List;

public class StoreIterator extends MergeIterator {
    private EditVersion editVersion;

    public StoreIterator(List<KeyValueIterator> iterators, EditVersion editVersion) {
        super(iterators);
        this.editVersion = editVersion;
    }

    @Override
    public void close() {
        editVersion.releaseReadReference();
    }
}
