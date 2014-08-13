-- This script covers multi-store queries.
register $PIGMIX_JAR
A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links);
B = foreach A generate user, action, (int)timespent as timespent, query_term,
    (double)estimated_revenue as estimated_revenue;
split B into C if user is not null, alpha if user is null;
split C into D if query_term is not null, aleph if query_term is null;
E = group D by user parallel $PARALLEL;
F = foreach E generate group, MAX(D.estimated_revenue);
store F into '$PIGMIX_OUTPUT/highest_value_page_per_user';
beta = group alpha by query_term parallel $PARALLEL;
gamma = foreach beta generate group, SUM(alpha.timespent);
store gamma into '$PIGMIX_OUTPUT/total_timespent_per_term';
beth = group aleph by action parallel $PARALLEL;
gimel = foreach beth generate group, COUNT(aleph);
store gimel into '$PIGMIX_OUTPUT/queries_per_action';
