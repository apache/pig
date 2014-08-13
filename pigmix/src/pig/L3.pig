--This script tests a join too large for fragment and replicate.  It also 
--contains a join followed by a group by on the same key, something that we
--could potentially optimize by not regrouping.
register $PIGMIX_JAR
A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links);
B = foreach A generate user, (double)estimated_revenue;
alpha = load '$HDFS_ROOT/users' using PigStorage('\u0001') as (name, phone, address,
        city, state, zip);
beta = foreach alpha generate name;
C = join beta by name, B by user parallel $PARALLEL;
D = group C by $0 parallel $PARALLEL;
E = foreach D generate group, SUM(C.estimated_revenue);
store E into '$PIGMIX_OUTPUT/L3out';

