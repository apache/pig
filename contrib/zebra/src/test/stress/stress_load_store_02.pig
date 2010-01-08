register $zebraJar;
--fs -rmr $outputDir



--a1 = LOAD '$inputDir/unsorted1' USING org.apache.hadoop.zebra.pig.TableLoader('int1,int2,str1,str2,byte1,byte2');
--limitedVals = LIMIT a1 10;
--dump limitedVals;

--store a1 into '$outputDir/mix1' using org.apache.hadoop.zebra.pig.TableStorer('[int1];[int2];[byte2];[str2,str1]');

--a2 = LOAD '$outputDir/mix1' USING org.apache.hadoop.zebra.pig.TableLoader('byte2,int2,int1,str1,str2');
--limitedVals = LIMIT a2 10;
--dump limitedVals;


--store a2 into '$outputDir/mix1_2' using org.apache.hadoop.zebra.pig.TableStorer('[int1];[int2];[byte2];[str2,str1]');      

a3 = LOAD '$outputDir/mix1_2' USING org.apache.hadoop.zebra.pig.TableLoader('byte2,int2,int1,str1,str2');
--limitedVals = LIMIT a2 10;
--dump limitedVals;


store a3 into '$outputDir/mix1_1' using org.apache.hadoop.zebra.pig.TableStorer('[int1];[int2];[byte2];[str2,str1]');   

--if only store once, and compare mix1 with mix1_1, table one has column number 6, table two has 5 (default column for table one)
--now we should compare mix1_1 and mix1_2 . they should be idential        
