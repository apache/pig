register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

a = load '2.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);
dump a;                
                      
a1order = order a1 by a;  
aorder = order a by a;   
       

store a1order into 'a1' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f,r1,m1]');    
store aorder into 'a2' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f,r1,m1]');

rec1 = load 'a1' using org.apache.hadoop.zebra.pig.TableLoader(); 
rec2 = load 'a2' using org.apache.hadoop.zebra.pig.TableLoader(); 
records1 = LOAD 'a1,a2' USING org.apache.hadoop.zebra.pig.TableLoader ('a,b,c,d,e,f,r1,m1', 'sorted');
--joina = join rec1 by a, rec2 by a2 using "merge" ;     
dump records1;
             
