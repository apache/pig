-- This script tests reading from a map, flattening a bag of maps, and use of bincond.
register $PIGMIX_JAR
A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links);
B = foreach A generate user, (int)action as action, (map[])page_info as page_info,
    flatten((bag{tuple(map[])})page_links) as page_links;
C = foreach B generate user,
    (action == 1 ? page_info#'a' : page_links#'b') as header;
D = group C by user parallel $PARALLEL;
E = foreach D generate group, COUNT(C) as cnt;
store E into '$PIGMIX_OUTPUT/L1out';


