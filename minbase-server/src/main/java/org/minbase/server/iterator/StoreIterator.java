package org.minbase.server.iterator;

import org.minbase.server.storage.version.EditVersion;

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
