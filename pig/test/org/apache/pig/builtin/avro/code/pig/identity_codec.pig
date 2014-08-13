SET avro.output.codec $CODEC
SET avro.mapred.deflate.level $LEVEL
in = LOAD '$INFILE' USING AvroStorage();
out = FOREACH in GENERATE *;
RMF $OUTFILE;
STORE out INTO '$OUTFILE' USING AvroStorage('$AVROSTORAGE_OUT_1','$AVROSTORAGE_OUT_2');
