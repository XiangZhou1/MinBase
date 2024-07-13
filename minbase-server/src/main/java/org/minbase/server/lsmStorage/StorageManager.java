package org.minbase.server.lsmStorage;

import org.minbase.server.conf.Config;
import org.minbase.server.constant.Constants;
import org.minbase.server.iterator.KeyValueIterator;
import org.minbase.server.op.Key;
import org.minbase.server.op.KeyValue;
import org.minbase.server.storage.edit.FileEdit;
import org.minbase.server.storage.sstable.SSTable;

import java.io.IOException;

public abstract class StorageManager extends ManiFest {
    public static final String Data_Dir = Config.get(Constants.KEY_DATA_DIR);
    protected long lastSequenceId = 0;
    public abstract void loadSSTables() throws IOException;

    public abstract KeyValue get(Key key);

    public abstract void addNewSSTable(SSTable ssTable, long lastSyncSequenceId) throws IOException;
    public abstract void saveSSTableFile(SSTable ssTable) throws IOException;

    public abstract KeyValueIterator iterator(Key startKey, Key endKey);

    public abstract FileEdit newFileEdit();

    public abstract void applyFileEdit(FileEdit fileEdit);

    public long getLastSequenceId() {
        return lastSequenceId;
    }

}
