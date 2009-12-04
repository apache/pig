register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

a2 = load '2.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);
       
b =  foreach a1  generate m1#'a' as ms1;
c =  foreach a2 generate m1#'b' as ms2;

--store b  into 'u11' using org.apache.hadoop.zebra.pig.TableStorer('');    
--store c  into 'u12' using org.apache.hadoop.zebra.pig.TableStorer('');    

records1 = LOAD 'u11,u12' USING org.apache.hadoop.zebra.pig.TableLoader ('ms1,a, ms2');
dump records1;
             
