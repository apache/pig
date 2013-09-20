package org.apache.pig.tools.pigstats;

import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.mapreduce.MRJobStats;

@Deprecated
/**
 * This class is only for backward compatibility. New application need
 * to use MRJobStats instead
 */
public class JobStats extends MRJobStats {
    public JobStats(String name, JobGraph plan) {
        super(name, plan);
    }
}
