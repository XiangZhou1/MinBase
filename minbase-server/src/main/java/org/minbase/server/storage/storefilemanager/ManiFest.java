package org.minbase.server.storage.storefilemanager;

import java.io.File;


public interface ManiFest {
    static final String manifestFileName = "manifest";

    void saveManifest();

    File getManiFestFile();
}
