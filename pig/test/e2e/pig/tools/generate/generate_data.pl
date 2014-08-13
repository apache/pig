#!/usr/bin/env perl
############################################################################           
#  Licensed to the Apache Software Foundation (ASF) under one or more                  
#  contributor license agreements.  See the NOTICE file distributed with               
#  this work for additional information regarding copyright ownership.                 
#  The ASF licenses this file to You under the Apache License, Version 2.0             
#  (the "License"); you may not use this file except in compliance with                
#  the License.  You may obtain a copy of the License at                               
#                                                                                      
#      http://www.apache.org/licenses/LICENSE-2.0                                      
#                                                                                      
#  Unless required by applicable law or agreed to in writing, software                 
#  distributed under the License is distributed on an "AS IS" BASIS,                   
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.            
#  See the License for the specific language governing permissions and                 
#  limitations under the License.                                                      
                                                                                       
# A utility to generate test data for pig test harness tests.
# 
#

use strict;
use charnames ();

our @firstName = ("alice", "bob", "calvin", "david", "ethan", "fred",
    "gabriella", "holly", "irene", "jessica", "katie", "luke", "mike", "nick",
    "oscar", "priscilla", "quinn", "rachel", "sarah", "tom", "ulysses", "victor",
    "wendy", "xavier", "yuri", "zach");

our @lastName = ("allen", "brown", "carson", "davidson", "ellison", "falkner",
    "garcia", "hernandez", "ichabod", "johnson", "king", "laertes", "miller",
    "nixon", "ovid", "polk", "quirinius", "robinson", "steinbeck", "thompson",
    "underhill", "van buren", "white", "xylophone", "young", "zipper");

############################################################################
# Explanation of rankedTuples: a pre-ranked set of tuples, each column meaning:
#   rownumber: simple RANK, sequential number
#	rankcabd: RANK BY c ASC , b DESC
#	rankbdaa: RANK BY b DESC, a ASC
#	rankbdca: RANK BY b DESC, c ASC
#	rankaacd: RANK BY a ASC , c DESC
#	rankaaba: RANK BY a ASC , b ASC
#	a,b,c:    values
#	tail:     long value in order to create multiple mappers
############################################################################
our @rankedTuples = (
	"1,21,5,7,1,1,0,8,8","2,26,2,3,2,5,1,9,10","3,30,24,21,2,3,1,3,10","4,6,10,8,3,4,1,7,2",
	"5,8,28,25,3,2,1,0,2","6,28,11,12,4,6,2,7,10","7,9,26,22,5,7,3,2,3","8,5,6,5,6,8,3,8,1",
	"9,29,16,15,7,9,4,6,10","10,18,12,10,8,11,5,7,6","11,14,17,14,9,10,5,6,5","12,6,12,8,10,11,5,7,2",
	"13,2,17,13,11,10,5,6,0","14,26,3,3,12,14,6,9,10","15,15,20,18,13,13,6,4,5","16,3,29,24,14,12,6,0,0",
	"17,23,21,19,15,16,7,4,8","18,19,19,16,16,17,7,5,6","19,20,30,26,16,15,7,0,6","20,12,21,17,17,16,7,4,4",
	"21,4,1,1,18,19,7,10,1","22,1,7,4,19,18,7,8,0","23,24,14,11,20,21,8,7,9","24,16,25,20,21,20,8,3,5",
	"25,25,27,23,22,22,9,1,9","26,21,8,7,23,25,9,8,8","27,17,4,2,24,26,9,9,6","28,10,8,6,25,25,9,8,4",
	"29,11,15,9,25,24,9,7,4","30,12,23,17,25,23,9,4,4");

sub randomName()
{
    return sprintf("%s %s", $firstName[int(rand(26))],
        $lastName[int(rand(26))]);
}

our @city = ("albuquerque", "bombay", "calcutta", "danville", "eugene",
    "frankfurt", "grenoble", "harrisburg", "indianapolis",
    "jerusalem", "kellogg", "lisbon", "marseilles",
    "nice", "oklohoma city", "paris", "queensville", "roswell",
    "san francisco", "twin falls", "umatilla", "vancouver", "wheaton",
    "xacky", "youngs town", "zippy");

sub randomCity()
{
    return $city[int(rand(26))];
}

our @state = ( "AL", "AK", "AS", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", 
    "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC",
    "ND", "OH", "OK", "OR", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA",
    "WA", "WV", "WI", "WY");

sub randomState()
{
    return $state[int(rand(50))];
}

our @classname = ("american history", "biology", "chemistry", "debate",
    "education", "forestry", "geology", "history", "industrial engineering",
    "joggying", "kindergarten", "linguistics", "mathematics", "nap time",
    "opthamology", "philosophy", "quiet hour", "religion", "study skills",
    "topology", "undecided", "values clariffication", "wind surfing", 
    "xylophone band", "yard duty", "zync studies");

sub randomClass()
{
    return $classname[int(rand(26))];
}

our @grade = ("A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D+", "D", "D-",
    "F");

sub randomGrade()
{
    return $grade[int(rand(int(@grade)))];
}

our @registration = ("democrat", "green", "independent", "libertarian",
    "republican", "socialist");

sub randomRegistration()
{
    return $registration[int(rand(int(@registration)))];
}

sub randomAge()
{
    return (int(rand(60)) + 18);
}

sub randomGpa()
{
    return rand(4.0);
}

our @street = ("A", "B", "C", "D", "E", "F", "G", "H", "I",
    "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S",
    "T", "U", "V", "W", "X", "Y", "Z");

sub randomStreet()
{
    return sprintf("%d %s st", int(rand(1000)), $street[int(rand(26))]);
}

sub randomZip()
{
    return int(rand(100000));
}

sub randomContribution()
{
    return sprintf("%.2f", rand(1000));
}

our @numLetter = ("1", "09", "09a");

sub randomNumLetter()
{
    return $numLetter[int(rand(int(@numLetter)))];
}

our @greekLetter = ( "alpha", "beta", "gamma", "delta", "epsilon", "zeta",
    "eta", "theta", "iota", "kappa", "lambda", "mu", "nu", "xi", "omicron",
    "pi", "rho", "sigma", "tau", "upsilon", "chi", "phi", "psi", "omega" );

sub randomGreekLetter()
{
    return $greekLetter[int(rand(int(@greekLetter)))];
}

sub randomNameAgeGpaMap()
{
    my $size = int(rand(3));
    my $map = "[";
    my @mapValues = ( "name#" . randomName(), "age#" . randomAge(), "gpa#" . randomGpa() );
    $size = ($size == 0 ? 1 : $size);
    for(my $i = 0; $i <= $size; $i++) {
        $map .= $mapValues[$i];
        if($i != $size) {
            $map .= ",";
        }
    }
    $map .= "]";
    return $map;
}

sub getMapFields($) {
    my $mapString = shift;
    # remove the enclosing square brackets
    $mapString =~ s/[\[\]]//g;
    # get individual map fields
    my @fields = split(/,/, $mapString);
    # get only the values 
    my $hash;
    for my $field (@fields) {
        if($field =~ /(\S+)#(.*)/) {
            $hash->{$1} = $2;
        }
    }
    return $hash;
}

sub randomNameAgeGpaTuple()
{
    my $gpa = sprintf("%0.2f", randomGpa());
    return "(" . randomName() . "," . randomAge() . "," . $gpa . ")" ;
}

sub randomNameAgeGpaBag()
{
    my $size = int(rand(int(3)));
    my $bag = "{";
    $size = ($size == 0 ? 1 : $size);
    for(my $i = 0; $i <= $size; $i++) {
        $bag .= randomNameAgeGpaTuple();
        if($i != $size) {
            $bag .= ",";
        }
    }
    $bag .= "}";
    return $bag;
}

our @textDoc = (
    "The cosmological proof, which we are now about to ex-",
    "amine, retains the connection of absolute necessity with the",
    "highest reality, but instead of reasoning, like the former proof,",
    "from the highest reality to necessity of existence, it reasons",
    "from the previously given unconditioned necessity of some",
    "being to the unlimited reality of that being. It thus enters upon",
    "a course of reasoning which, whether rational or only pseudo-",
    "rational, is at any rate natural, and the most convincing not",
    "only for common sense but even for speculative understand-",
    "ing. It also sketches the first outline of all the proofs in natural",
    "theology, an outline which has always been and always will",
    "be followed, however much embellished and disguised by",
    "superfluous additions. This proof, termed by Leibniz the proof",
    "a contingentia mundi, we shall now proceed to expound and",
    "examine.");

sub usage()
{
    warn "Usage: $0 filetype numrows tablename targetdir [nosql]\n";
    warn "\tValid filetypes [studenttab, studentcolon, \n";
    warn "\t\tstudentnulltab, studentcomplextab, studentctrla, voternulltab\n";
    warn "\t\tvotertab, reg1459894, textdoc, unicode, manual]\n";
}

our @greekUnicode = ("\N{U+03b1}", "\N{U+03b2}", "\N{U+03b3}", "\N{U+03b4}",
    "\N{U+03b5}", "\N{U+03b6}", "\N{U+03b7}", "\N{U+03b8}", "\N{U+03b9}",
    "\N{U+03ba}", "\N{U+03bb}", "\N{U+03bc}", "\N{U+03bd}", "\N{U+03be}",
    "\N{U+03bf}", "\N{U+03c0}", "\N{U+03c1}", "\N{U+03c2}", "\N{U+03c3}",
    "\N{U+03c4}", "\N{U+03c5}", "\N{U+03c6}", "\N{U+03c7}", "\N{U+03c8}",
    "\N{U+03c9}");

sub randomUnicodeNonAscii()
{
    my $name = $firstName[int(rand(int(@firstName)))] .
         $greekUnicode[int(rand(int(@greekUnicode)))];
    return $name;
}

my $testvar = "\N{U+03b1}\N{U+03b3}\N{U+03b1}\N{U+03c0}\N{U+03b7}";

sub getBulkCopyCmd(){
        my $sourceDir= shift;
        my $tableName = shift;
        my $delimeter = shift;
        $delimeter = '\t' if ( !$delimeter );

#               . "\nCOPY $tableName FROM \'$sourceDir/$tableName' using DELIMITERS \'". '\t' . "\' WITH NULL AS '\n';";

        my $cmd= "\nbegin transaction;" 
                  . "\nCOPY $tableName FROM \'$sourceDir/$tableName' using DELIMITERS \'$delimeter\';" 
                  . "\ncommit;"
                  . "\n";

        return $cmd;
}


# main($)
{
    # explicitly call srand so we get the same data every time
    # we generate it.  However, we set it individually for each table type.
    # Otherwise we'd be generating the same data sets regardless of size,
    # and this would really skew our joins.

    my $filetype = shift;
    my $numRows = shift;
    my $tableName = shift;
    my $targetDir= shift;
    my $nosql = shift;

    die usage() if (!defined($filetype) || !defined($numRows));

    if ($numRows <= 0) { usage(); }

    if ( $targetDir ) {
       open(HDFS, "> $targetDir/$tableName") or die("Cannot open file $tableName, $!\n");
       open(PSQL, "> $targetDir/$tableName.sql") or die("Cannot open file $tableName.sql, $!\n") unless defined $nosql;
    } else {
       open(HDFS, "> $tableName") or die("Cannot open file $tableName, $!\n");
       open(PSQL, "> $tableName.sql") or die("Cannot open file $tableName.sql, $!\n") unless defined $nosql;
    }

    if ($filetype eq "manual") {
    } elsif ($filetype eq "studenttab") {
        srand(3.14159 + $numRows);
        print PSQL "create table $tableName (name varchar(100), age integer, gpa float(3));\n" unless defined $nosql;
        print PSQL &getBulkCopyCmd( $targetDir, $tableName ) unless defined $nosql;
        for (my $i = 0; $i < $numRows; $i++) {
            my $name = randomName();
            my $age = randomAge();
            my $gpa = randomGpa();
            printf HDFS "%s\t%d\t%.2f\n", $name, $age, $gpa;
        }

    } elsif ($filetype eq "studentnulltab") {
        srand(3.14159 + $numRows);
        print PSQL "create table $tableName (name varchar(100), age integer, gpa float(3));\n";
        print PSQL "begin transaction;\n";
        for (my $i = 0; $i < $numRows; $i++) {
            # generate nulls in a random fashion
            my $name = rand(1) < 0.05 ? '' : randomName();
            my $age = rand(1) < 0.05 ? '' : randomAge();
            my $gpa = rand(1) < 0.05 ? '' : randomGpa();
            printf PSQL "insert into $tableName (name, age, gpa) values(";
            print PSQL ($name eq ''? "null, " : "'$name', "), ($age eq ''? "null, " : "$age, ");
            if($gpa eq '') {
                print PSQL "null);\n"
            } else {
                printf PSQL "%.2f);\n", $gpa;    
            }
            print HDFS "$name\t$age\t";
            if($gpa eq '') {
                print HDFS "\n"
            } else {
                printf HDFS "%.2f\n", $gpa;    
            }
            
        }
        print PSQL "commit;\n" unless defined $nosql;

    } elsif ($filetype eq "studentcolon") {
        srand(2.718281828459 + $numRows);
        print PSQL "create table $tableName (name varchar(100), age integer, gpa float(3));\n" unless defined $nosql;
        print PSQL &getBulkCopyCmd( $targetDir, $tableName, ':' ) unless defined $nosql;
        for (my $i = 0; $i < $numRows; $i++) {
            my $name = randomName();
            my $age = randomAge();
            my $gpa = randomGpa();
            printf HDFS "%s:%d:%.2f\n", $name, $age, $gpa;
=begin
    } elsif ($filetype eq "studentusrdef") {
        srand(6.62606896 + $numRows);
        for (my $i = 0; $i < $numRows; $i++) {
            # TODO need to add SQL info.
            printf("%s,%d,%.2f,", randomName(), randomAge(), randomGpa());
            printf("<%s,%s,%s,%d>,", randomStreet(), randomCity(), randomState(),
                randomZip());
            printf("[%s:<%s,%s>],", randomClass(), randomClass(), randomName());
            printf("{");
            my $elementsInBag = int(rand(100));
            for (my $j = 0; $j < $elementsInBag; $j++) {
                if ($j != 0) { printf(","); }
                printf("<%s,%s,%s>", randomClass(), randomName(), randomGrade());
            }
            printf("}\n");
        }
=cut
        }
        print PSQL "commit;\n" unless defined $nosql;

    } elsif ($filetype eq "studentctrla") {
        srand(6.14159 + $numRows);
        print PSQL "create table $tableName (name varchar(100), age integer, gpa float(3));\n";
        print PSQL "begin transaction;\n";
        for (my $i = 0; $i < $numRows; $i++) {
            my $name = randomName();
            my $age = randomAge();
            my $gpa = randomGpa();
            printf PSQL "insert into $tableName (name, age, gpa) values('%s', %d, %.2f);\n",
                $name, $age, $gpa;
            printf HDFS "%s%d%.2f\n", $name, $age, $gpa;
        }
        print PSQL "commit;\n" unless defined $nosql;


    } elsif ($filetype eq "studentcomplextab") {
        srand(3.14159 + $numRows);
        print PSQL "create table $tableName (nameagegpamap varchar(500), nameagegpatuple varchar(500), nameagegpabag varchar(500), nameagegpamap_name varchar(500), nameagegpamap_age integer, nameagegpamap_gpa float(3));\n";
        print PSQL "begin transaction;\n";
        for (my $i = 0; $i < $numRows; $i++) {
            # generate nulls in a random fashion
            my $map = rand(1) < 0.05 ? '' : randomNameAgeGpaMap();
            my $tuple = rand(1) < 0.05 ? '' : randomNameAgeGpaTuple();
            my $bag = rand(1) < 0.05 ? '' : randomNameAgeGpaBag();
            printf PSQL "insert into $tableName (nameagegpamap, nameagegpatuple, nameagegpabag, nameagegpamap_name, nameagegpamap_age, nameagegpamap_gpa) values(";
            my $mapHash;
            if($map ne '') {
                $mapHash = getMapFields($map);
            }

            print PSQL ($map eq ''? "null, " : "'$map', "), 
                        ($tuple eq ''? "null, " : "'$tuple', "),
                        ($bag eq '' ? "null, " : "'$bag', "),
                        ($map eq '' ? "null, " : (exists($mapHash->{'name'}) ? "'".$mapHash->{'name'}."', " : "null, ")),
                        ($map eq '' ? "null, " : (exists($mapHash->{'age'}) ? "'".$mapHash->{'age'}."', " : "null, ")),
                        ($map eq '' ? "null);\n" : (exists($mapHash->{'gpa'}) ? "'".$mapHash->{'gpa'}."');\n" : "null);\n"));
            print HDFS "$map\t$tuple\t$bag\n";
        }
        print PSQL "commit;\n" unless defined $nosql;

    } elsif ($filetype eq "votertab") {
        srand(299792458 + $numRows);
        print PSQL "create table $tableName (name varchar(100), age integer, registration varchar(20), contributions float);\n" unless defined $nosql;
        print PSQL &getBulkCopyCmd( $targetDir, $tableName ) unless defined $nosql;
        for (my $i = 0; $i < $numRows; $i++) {
            my $name = randomName();
            my $age = randomAge();
            my $registration = randomRegistration();
            my $contribution = randomContribution();
            printf HDFS "%s\t%d\t%s\t%.2f\n", $name, $age,
                $registration, $contribution;
        }

    } elsif ($filetype eq "voternulltab") {
        srand(299792458 + $numRows);
        print PSQL "create table $tableName (name varchar(100), age integer, registration varchar(20), contributions float);\n" unless defined $nosql;
        print PSQL "begin transaction;\n" unless defined $nosql;
        for (my $i = 0; $i < $numRows; $i++) {
            # generate nulls in a random fashion
            my $name = rand(1) < 0.05 ? '' : randomName();
            my $age = rand(1) < 0.05 ? '' : randomAge();
            my $registration = rand(1) < 0.05 ? '' : randomRegistration();
            my $contribution = rand(1) < 0.05 ? '' : randomContribution();
            printf PSQL "insert into $tableName (name, age, registration, contributions) values(";
            print PSQL ($name eq ''? "null, " : "'$name', "), 
                            ($age eq ''? "null, " : "$age, "),
                            ($registration eq ''? "null, " : "'$registration', ");
            if($contribution eq '') {
                print PSQL "null);\n"
            } else {
                printf PSQL "%.2f);\n", $contribution;    
            }
            print HDFS "$name\t$age\t$registration\t";
            if($contribution eq '') {
                print HDFS "\n"
            } else {
                printf HDFS "%.2f\n", $contribution;    
            }
        }
        print PSQL "commit;\n" unless defined $nosql;

    } elsif ($filetype eq "reg1459894") {
        srand(6.67428 + $numRows);
        print PSQL "create table $tableName (first varchar(10), second varchar(10));\n" unless defined $nosql;
        print PSQL &getBulkCopyCmd( $targetDir, $tableName ) unless defined $nosql;
        for (my $i = 0; $i < $numRows; $i++) {
            my $letter = randomNumLetter(); 
            my $gkLetter = randomGreekLetter(); 
            printf HDFS "%s\t%s\n", $letter, $gkLetter;
        }

    } elsif ($filetype eq "textdoc") {
        # This one ignores the number of lines.  It isn't random either.
        print PSQL "create table $tableName (name varchar(255));\n" unless defined $nosql;
        print PSQL "begin transaction;\n" unless defined $nosql;
        for (my $i = 0; $i < @textDoc; $i++) {
            my $sqlWords = $textDoc[$i];
            $sqlWords =~ s/([\w-]+)/$1,/g;
            print PSQL  "insert into $tableName (name) values('($sqlWords)');\n" unless defined $nosql;
            print HDFS "$textDoc[$i]\n";
        }
        print PSQL "commit;\n" unless defined $nosql;


    } elsif ($filetype eq "unicode") {
        srand(1.41421 + $numRows);
        print PSQL "create table $tableName (name varchar(255));\n" unless defined $nosql;
        print PSQL "begin transaction;\n" unless defined $nosql;
        for (my $i = 0; $i < $numRows; $i++) {
            my $name = randomUnicodeNonAscii(); 
            printf PSQL "insert into $tableName (name) values('%s');\n",
                $name unless defined $nosql;
            printf HDFS "%s\n", $name;
        }
        print PSQL "commit;\n" unless defined $nosql;

    } elsif ($filetype eq "allscalar") {
        srand(1228.2011 + $numRows);
        for (my $i = 0; $i < $numRows; $i++) {
            my $name = rand(1) < 0.05 ? '' : randomName();
            my $age = rand(1) < 0.05 ? '' : randomAge();
            my $gpa = rand(1) < 0.05 ? '' : randomGpa();
            my $instate = rand(1) < 0.05 ? '' : (rand(1) < 0.5 ? 'true' : 'false');
            printf HDFS "%s\t%d\t%.2f\t%s\n", $name, $age, $gpa, $instate;
        }
    } elsif ($filetype eq "numbers") {
        srand(2012 + $numRows);
        for (my $i = 0; $i < $numRows; $i++) {
            my $tid = ($i/1000+1) * 1000;
            my $rand5 = int(rand(5)) + 1;
            my $rand100 = int(rand(100)) + 1;
            my $rand1000 = int(rand(1000)) + 1;
            my $randf = rand(10);
            printf HDFS "%d:%d:%d:%d:%d:%dL:%.2ff:%.2f\n", $tid, $i, $rand5, $rand100, $rand1000, $rand1000, $randf, $randf;
        }
    }  elsif ($filetype eq "ranking") {
        for (my $i = 0; $i < $numRows; $i++) {
            my $tuple = $rankedTuples[int($i)];
            printf HDFS "$tuple,";
            for my $j ( 0 .. 1000000) {
				printf HDFS "%d",$j;
			}
			printf HDFS "\n";
        }
    } elsif ($filetype eq "biggish") {
        for (my $i = 1; $i < $numRows; $i++) {
            printf HDFS "$i,$i,";
            for my $j ( 0 .. 1000) {
				printf HDFS "%d",$j;
            }
            printf HDFS "\n";
        }
    } else {
        warn "Unknown filetype $filetype\n";
        usage();
    }
}


