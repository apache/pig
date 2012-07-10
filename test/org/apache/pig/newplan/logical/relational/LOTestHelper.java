package org.apache.pig.newplan.logical.relational;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.LoadFunc;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;

public class LOTestHelper {
    public static LOLoad newLOLoad(FileSpec loader, LogicalSchema schema, LogicalPlan plan, Configuration conf) {
        LoadFunc loadFunc = null;
        if (loader != null) {
            loadFunc = (LoadFunc)PigContext.instantiateFuncFromSpec(loader.getFuncSpec());
        }
        return new LOLoad(loader, schema, plan, conf, loadFunc, null);
    }
}
