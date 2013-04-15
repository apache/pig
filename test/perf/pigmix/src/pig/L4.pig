-- This script covers foreach/generate with a nested distinct.
register $PIGMIX_JAR
A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links);
B = foreach A generate user, action;
C = group B by user parallel $PARALLEL;
D = foreach C {
    aleph = B.action;
    beth = distinct aleph;
    generate group, COUNT(beth);
}
store D into '$PIGMIX_OUTPUT/L4out';
