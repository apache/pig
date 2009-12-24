register $zebraJar;
--fs -rmr $outputDir



a1 = LOAD '$inputDir/unsorted1' USING org.apache.hadoop.zebra.pig.TableLoader('c1');
--limitedVals = LIMIT a1 10;
--dump limitedVals;

store a1 into '$outputDir/c1' using org.apache.hadoop.zebra.pig.TableStorer('[c1]');    

a2 = LOAD '$outputDir/c1' USING org.apache.hadoop.zebra.pig.TableLoader('c1');
--limitedVals = LIMIT a2 10;
--dump limitedVals;


store a2 into '$outputDir/c1_2' using org.apache.hadoop.zebra.pig.TableStorer('[c1]');    
  
