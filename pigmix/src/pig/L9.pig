--This script covers order by of a single value.
register $PIGMIX_JAR
A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links);
B = order A by query_term parallel $PARALLEL;
store B into '$PIGMIX_OUTPUT/L9out';
