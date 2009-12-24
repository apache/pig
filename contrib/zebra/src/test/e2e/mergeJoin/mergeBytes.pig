register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

a = load '2.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);
--dump a1;                
                      
a1order = order a1 by f;  
aorder = order a by f;   
dump a1order;       

store a1order into 'f1' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f,r1,m1]');    
store aorder into 'f2' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f,r1,m1]');

rec1 = load 'f1' using org.apache.hadoop.zebra.pig.TableLoader(); 
rec2 = load 'f2' using org.apache.hadoop.zebra.pig.TableLoader(); 
--records1 = LOAD 'f1,f2' USING org.apache.hadoop.zebra.pig.TableLoader ('source_table,f,m1,a,b,c,d,e', 'sorted');
joina = join rec1 by f, rec2 by f using "merge" ;     
dump joina;
             
