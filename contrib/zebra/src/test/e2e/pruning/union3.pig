register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

a2 = load '2.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);
       
c =  foreach a2 generate d,e,f;

store a1  into 'u31' using org.apache.hadoop.zebra.pig.TableStorer('');    
store c  into 'u32' using org.apache.hadoop.zebra.pig.TableStorer('');    

records1 = LOAD 'u31,u32' USING org.apache.hadoop.zebra.pig.TableLoader ();
dump records1;
             
