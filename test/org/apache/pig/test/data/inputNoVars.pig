
register /home/y/lib/java/pigtest/testudf.jar;
A = load '/user/pig/tests/data/singlefile/textdoc' using TextLoader();
define X `perl -ne 'chomp $_; print "$_\n"'` output (stdout using org.apache.pig.test.udf.storefunc.StringStore());
B = stream A through X;
store B into '/user/pig/tests/results/olgan.1209067990/DefineClause_4.out';
