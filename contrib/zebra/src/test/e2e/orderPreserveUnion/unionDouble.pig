register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

a = load '2.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);
--dump a;                
                      
a1order = order a1 by d;  
aorder = order a by d;   
       

store a1order into 'd1' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f,r1,m1]');    
store aorder into 'd' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f,r1,m1]');

rec1 = load 'd1' using org.apache.hadoop.zebra.pig.TableLoader(); 
rec2 = load 'd' using org.apache.hadoop.zebra.pig.TableLoader(); 
records1 = LOAD 'd1,d' USING org.apache.hadoop.zebra.pig.TableLoader ('d,e,f,m1,a,b,c', 'sorted');
--joina = join rec1 by a, rec2 by a using "merge" ;     
dump records1;
             
