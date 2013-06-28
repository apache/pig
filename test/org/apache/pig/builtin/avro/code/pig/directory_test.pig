in = LOAD '$INFILE' USING AvroStorage;
out = FOREACH (GROUP in ALL) GENERATE 
    (int) SUM(in.item) as itemSum:int,
    (int) COUNT_STAR(in) as n:int;
RMF $OUTFILE;
STORE out INTO '$OUTFILE' USING AvroStorage('$AVROSTORAGE_OUT_1','$AVROSTORAGE_OUT_2');