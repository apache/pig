---------------------------------------------------------------------
-- Top N Queries.
---------------------------------------------------------------------

data =
    LOAD '$input'
    AS (query:CHARARRAY, count:INT);

queries_group = 
    GROUP data 
    BY query
    PARALLEL $reducers;

queries_sum = 
    FOREACH queries_group 
    GENERATE 
        group AS query, 
        SUM(data.count) AS count;

queries_ordered = 
    ORDER queries_sum 
    BY count DESC
    PARALLEL $reducers;
            
queries_limit = LIMIT queries_ordered $n;

STORE queries_limit INTO '$output';
