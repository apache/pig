register /grid/0/dev/hadoopqa/jars/zebra.jar;

a1 = LOAD '/data/SDS_HTable' USING org.apache.hadoop.zebra.pig.TableLoader('MLF_viewinfo');
--limitedVals = LIMIT a1 10;
--dump limitedVals;

store a1 into '/data/collection_viewinfo1' using org.apache.hadoop.zebra.pig.TableStorer('[MLF_viewinfo]');    

a2 = LOAD '/data/collection_viewinfo1' USING org.apache.hadoop.zebra.pig.TableLoader('MLF_viewinfo');
--limitedVals = LIMIT a2 10;
--dump limitedVals;
                      

store a2 into '/data/collection_viewinfo2' using org.apache.hadoop.zebra.pig.TableStorer('[MLF_viewinfo]');    

             
