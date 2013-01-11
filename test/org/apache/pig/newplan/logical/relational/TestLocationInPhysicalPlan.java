package org.apache.pig.newplan.logical.relational;

import java.io.File;
import java.util.List;

import junit.framework.Assert;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator.OriginalLocation;
import org.apache.pig.test.Util;
import org.apache.pig.tools.pigstats.JobStats;
import org.junit.Test;

public class TestLocationInPhysicalPlan {

    @Test
    public void test() throws Exception {
        File input = File.createTempFile("test", "input");
        input.deleteOnExit();
        File output = File.createTempFile("test", "output");
        output.delete();
        Util.createLocalInputFile(input.getAbsolutePath(), new String[] {
            "1,2,3",
            "1,1,3",
            "1,1,1",
            "3,1,1",
            "1,2,1",
        });
        PigServer pigServer = new PigServer(ExecType.LOCAL);
        pigServer.setBatchOn();
        pigServer.registerQuery(
                "A = LOAD '" + input.getAbsolutePath() + "' using PigStorage();\n"
            +  	"B = GROUP A BY $0;\n"
            + 	"A = FOREACH B GENERATE COUNT(A);\n"
            +	"STORE A INTO '" + output.getAbsolutePath() + "';");
        ExecJob job = pigServer.executeBatch().get(0);
        List<OriginalLocation> originalLocations = job.getPOStore().getOriginalLocations();
        Assert.assertEquals(1, originalLocations.size());
        OriginalLocation originalLocation = originalLocations.get(0);
        Assert.assertEquals(4, originalLocation.getLine());
        Assert.assertEquals(0, originalLocation.getOffset());
        Assert.assertEquals("A", originalLocation.getAlias());
        JobStats jStats = (JobStats)job.getStatistics().getJobGraph().getSinks().get(0);
        Assert.assertEquals("M: A[1,4],A[3,4],B[2,4] C: A[3,4],B[2,4] R: A[3,4]", jStats.getAliasLocation());
    }
}
