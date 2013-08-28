#!/bin/sh

if [[ -z $PIG_HOME ]] 
then
    echo "Please set PIG_HOME environment variable to where pig-withouthadoop.jar located."
    exit 1
fi

if [[ -z $PIG_BIN ]]
then
    echo "Please set PIG_BIN environment variable to the pig script (usually $PIG_HOME/bin/pig)"
    exit 1
fi

if [[ -z $PIGMIX_HOME ]]
then
    export PIGMIX_HOME=$PIG_HOME/test/perf/pigmix
fi

if [[ -z $HADOOP_HOME ]]
then
    echo "Please set HADOOP_HOME environment variable, make sure $HADOOP_HOME/bin/hadoop exists"
    exit 1
fi

source $PIGMIX_HOME/conf/config.sh

pigjar=$PIG_HOME/pig-withouthadoop.jar
pigmixjar=$PIGMIX_HOME/pigmix.jar

classpath=$pigjar:$pigmixjar

export HADOOP_CLASSPATH=$classpath

export PIG_OPTS="-Xmx1024m"
export HADOOP_CLIENT_OPTS="-Xmx1024m"

if [ $HADOOP_VERSION == "23" ]; then
    echo "Going to run $HADOOP_HOME/bin/hadoop fs -mkdir -p $hdfsroot"
    $HADOOP_HOME/bin/hadoop fs -mkdir -p $hdfsroot
else
    echo "Going to run $HADOOP_HOME/bin/hadoop fs -mkdir $hdfsroot"
    $HADOOP_HOME/bin/hadoop fs -mkdir $hdfsroot
fi

if [ $? -ne 0 ]
then
    echo "Fail to run $HADOOP_HOME/bin/hadoop fs -mkdir $hdfsroot, perhaps the destination already exist, if so, you need to run:"
    echo "$HADOOP_HOME/bin/hadoop fs -rmr $hdfsroot"
    exit 1
fi

mainclass=org.apache.pig.test.pigmix.datagen.DataGenerator 

user_field=s:20:1600000:z:7
action_field=i:1:2:u:0
os_field=i:1:20:z:0
query_term_field=s:10:1800000:z:20
ip_addr_field=l:1:1000000:z:0
timestamp_field=l:1:86400:z:0
estimated_revenue_field=d:1:100000:z:5
page_info_field=m:10:1:z:0
page_links_field=bm:10:1:z:20

echo "Generating $pages"

pages=$hdfsroot/page_views

$HADOOP_HOME/bin/hadoop jar $pigmixjar $mainclass \
    -m $mappers -r $rows -f $pages $user_field \
    $action_field $os_field $query_term_field $ip_addr_field \
    $timestamp_field $estimated_revenue_field $page_info_field \
    $page_links_field


# Skim off 1 in 10 records for the user table
# Be careful the file is in HDFS if you run previous job as hadoop job, 
# you should either copy data into local disk to run following script
# or run hadoop job to trim the data

protousers=$hdfsroot/protousers
echo "Skimming users"
$PIG_BIN << EOF
register $pigmixjar;
A = load '$pages' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user;
C = distinct B parallel $mappers;
D = order C by \$0 parallel $mappers;
store D into '$protousers';
EOF


# Create users table, with now user field.
phone_field=s:10:1600000:z:20
address_field=s:20:1600000:z:20
city_field=s:10:1600000:z:20
state_field=s:2:1600:z:20
zip_field=i:2:1600:z:20

users=$hdfsroot/users

echo "Generating $users"

$HADOOP_HOME/bin/hadoop jar $pigmixjar $mainclass \
    -m $mappers -i $protousers -f $users $phone_field \
    $address_field $city_field $state_field $zip_field

# Find unique keys for fragment replicate join testing
# If file is in HDFS, extra steps are required
numuniquekeys=500
protopowerusers=$hdfsroot/proto_power_users
echo "Skimming power users"

$PIG_BIN << EOF
register $pigmixjar;
fs -rmr $protopowerusers;
A = load '$pages' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user;
C = distinct B parallel $mappers;
D = order C by \$0 parallel $mappers;
E = limit D 500;
store E into '$protopowerusers';
EOF

echo "Generating $powerusers"

powerusers=$hdfsroot/power_users

$HADOOP_HOME/bin/hadoop jar $pigmixjar $mainclass \
    -m $mappers -i $protopowerusers -f $powerusers $phone_field \
    $address_field $city_field $state_field $zip_field

echo "Generating widerow"

widerows=$hdfsroot/widerow
user_field=s:20:10000:z:0
int_field=i:1:10000:u:0 

$HADOOP_HOME/bin/hadoop jar $pigmixjar $mainclass \
    -m $mappers -r $widerowcnt -f $widerows $user_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field

widegroupbydata=$hdfsroot/widegroupbydata

$PIG_BIN << EOF
register $pigmixjar;
A = load '$pages' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = order A by user parallel $mappers;
store B into '${pages}_sorted' using PigStorage('\u0001');

exec

alpha = load '$users' using PigStorage('\u0001') as (name, phone, address, city, state, zip);
a1 = order alpha by name parallel $mappers;
store a1 into '${users}_sorted' using PigStorage('\u0001');

exec

a = load '$powerusers' using PigStorage('\u0001') as (name, phone, address, city, state, zip);
b = sample a 0.5;
store b into '${powerusers}_samples' using PigStorage('\u0001');

exec

A = load '$pages' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader() 
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links,
user as user1, action as action1, timespent as timespent1, query_term as query_term1, ip_addr as ip_addr1, timestamp as timestamp1, estimated_revenue as estimated_revenue1, page_info as page_info1, page_links as page_links1,
user as user2, action as action2, timespent as timespent2, query_term as query_term2, ip_addr as ip_addr2, timestamp as timestamp2, estimated_revenue as estimated_revenue2, page_info as page_info2, page_links as page_links2;
store B into '$widegroupbydata' using PigStorage('\u0001');
EOF

poweruserlocal=$localtmp/$powerusers
poweruserlocalsingle=$localtmp/${powerusers}_single
rm -fr $poweruserlocal
rm $poweruserlocalsingle
mkdir -p $poweruserlocal
$PIG_BIN << EOF
fs -copyToLocal ${powerusers}/* $poweruserlocal;
EOF

cat $poweruserlocal/* > poweruserlocalsingle

$PIG_BIN << EOF
fs -rmr $protousers;
fs -rmr $powerusers;
fs -rmr $protopowerusers;
fs -copyFromLocal poweruserlocalsingle $hdfsroot/power_users;
EOF

rm -fr $poweruserlocal
rm poweruserlocalsingle
