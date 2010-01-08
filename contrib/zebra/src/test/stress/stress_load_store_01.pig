register $zebraJar;
fs -rmr $outputDir

a1 = LOAD '$inputDir/$unsorted1' USING org.apache.hadoop.zebra.pig.TableLoader();
--limitedVals = LIMIT a1 10;
--dump limitedVals;

store a1 into '$outputDir/store1' using org.apache.hadoop.zebra.pig.TableStorer('');    

a2 = LOAD '$outputDir/store1' USING org.apache.hadoop.zebra.pig.TableLoader();
--limitedVals = LIMIT a2 10;
--dump limitedVals;


store a2 into '$outputDir/store2' using org.apache.hadoop.zebra.pig.TableStorer('');    

                
