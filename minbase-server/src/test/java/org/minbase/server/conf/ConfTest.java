package org.minbase.server.conf;

import org.junit.Test;
import org.minbase.server.constant.Constants;

public class ConfTest {
    @Test
    public void test1(){
        System.out.println(Config.get(Constants.KEY_COMPACTION_STRATEGY));
    }
}
