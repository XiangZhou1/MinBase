package org.minbase.server.lsmStorage;

import java.io.File;

import static org.minbase.server.lsmStorage.StorageManager.Data_Dir;


public abstract class ManiFest {
    protected static final String manifestFileName = "manifest";

    public abstract void saveManifest();

    public File getManiFestFile() {
        return new File(Data_Dir + File.separator + "level" + File.separator + manifestFileName);
    }
}
