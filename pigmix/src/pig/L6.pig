-- This script covers the case where the group by key is a significant
-- percentage of the row.
register $PIGMIX_JAR
A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links);
B = foreach A generate user, action, (int)timespent as timespent, query_term, ip_addr, timestamp;
C = group B by (user, query_term, ip_addr, timestamp) parallel $PARALLEL;
D = foreach C generate flatten(group), SUM(B.timespent);
store D into '$PIGMIX_OUTPUT/L6out';

