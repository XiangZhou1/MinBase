package org.minbase.server.iterator;

import org.minbase.server.storage.edit.EditVersion;

import java.util.List;

public class StorageIterator extends MergeIterator {
    private EditVersion editVersion;

    public StorageIterator(List<KeyValueIterator> iterators, EditVersion editVersion) {
        super(iterators);
        this.editVersion = editVersion;
    }

    @Override
    public void close() {
        editVersion.releaseReadReference();
    }
}
