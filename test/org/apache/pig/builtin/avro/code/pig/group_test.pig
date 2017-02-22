in = LOAD '$INFILE' USING AvroStorage();
grouped = GROUP in BY (value1.thing);
flattened = FOREACH grouped GENERATE flatten(in) as (key: chararray,value1: (thing: chararray,count: int),value2: (thing: chararray,count: int));
RMF $OUTFILE;
STORE flattened INTO '$OUTFILE' USING AvroStorage();
