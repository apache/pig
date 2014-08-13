-- This script covers distinct and union.
register $PIGMIX_JAR
A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links);
B = foreach A generate user;
C = distinct B parallel $PARALLEL;
alpha = load '$HDFS_ROOT/widerow' using PigStorage('\u0001');
beta = foreach alpha generate $0 as name;
gamma = distinct beta parallel $PARALLEL;
D = union C, gamma;
E = distinct D parallel $PARALLEL;
store E into '$PIGMIX_OUTPUT/L11out';
