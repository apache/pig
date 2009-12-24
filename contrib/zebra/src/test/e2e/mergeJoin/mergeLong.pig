register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

a = load '2.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);
--dump a;                
                      
a1order = order a1 by c;  
aorder = order a by c;   
       

store a1order into 'c1' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f,r1,m1]');    
store aorder into 'c2' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f,r1,m1]');

rec1 = load 'c1' using org.apache.hadoop.zebra.pig.TableLoader(); 
rec2 = load 'c2' using org.apache.hadoop.zebra.pig.TableLoader(); 
--records1 = LOAD 'c1,c2' USING org.apache.hadoop.zebra.pig.TableLoader ('c,d,e,f,r1,m1,a,b', 'sorted');
joina = join rec1 by c, rec2 by c using "merge" ;     
--dump records1;
dump joina;
             
