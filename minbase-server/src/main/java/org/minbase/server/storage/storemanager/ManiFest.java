package org.minbase.server.storage.storemanager;

import java.io.File;


public interface ManiFest {
    static final String manifestFileName = "manifest";

    void saveManifest();

    File getManiFestFile();
}
