#!/usr/bin/env perl

#print total line number of the file
$n=0;
while ($line = <>) {
$n++;
print "$n). $line>\n";
}
print "There are ($n) lines in the file\n";
