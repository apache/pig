register $zebraJar;
fs -rmr $outputDir

a1 = LOAD '$inputDir/$unsorted1' USING org.apache.hadoop.zebra.pig.TableLoader('m1');
--limitedVals = LIMIT a1 10;
--dump limitedVals;

store a1 into '$outputDir/store1' using org.apache.hadoop.zebra.pig.TableStorer('[m1]');    

a2 = LOAD '$outputDir/store1' USING org.apache.hadoop.zebra.pig.TableLoader('m1');
--limitedVals = LIMIT a2 10;
--dump limitedVals;


store a2 into '$outputDir/store2' using org.apache.hadoop.zebra.pig.TableStorer('[m1]');    

                
