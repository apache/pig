--This script does an anti-join.  This is useful because it is a use of
--cogroup that is not a regular join.
register $PIGMIX_JAR
A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links);
B = foreach A generate user;
alpha = load '$HDFS_ROOT/users' using PigStorage('\u0001') as (name, phone, address,
        city, state, zip);
beta = foreach alpha generate name;
C = cogroup beta by name, B by user parallel $PARALLEL;
D = filter C by COUNT(beta) == 0;
E = foreach D generate group;
store E into '$PIGMIX_OUTPUT/L5out';
