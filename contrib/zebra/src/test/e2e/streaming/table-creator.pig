-- The script converts wikipedia pagecounts file into a 
-- zebra table

register /grid/0/dev/hadoopqa/jars/zebra.jar;
pagecounts = LOAD 'pagecounts-20090922-100000' USING PigStorage(' ') AS (project, page, count, size);

-- covert to a zebra table

STORE pagecounts INTO 'pagecounts-table' USING org.apache.hadoop.zebra.pig.TableStorer('');

