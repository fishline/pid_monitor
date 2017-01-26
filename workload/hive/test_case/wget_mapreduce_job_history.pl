#!/usr/bin/perl
use warnings;
use strict;

if ($#ARGV != 1) {
    print "Usage: ./wget_mapreduce_job_history.pl <JOB_ID> <FOLDER>\n";
    exit 1;
}

my $job_id = $ARGV[0];
my $tgt_folder = $ARGV[1];
`rm -rf 9.40.201.182:19888`;
`wget -r -l2 -R hadoop-root-namenode-namenode.log,hadoop-root-secondarynamenode-namenode.log,mapred-root-historyserver-opperfx10.aus.stglabs.ibm.com.log,yarn-root-resourcemanager-namenode.log,mapred-root-historyserver-namenode.log,mapred-root-historyserver-master.log,job_*_* http://9.40.201.182:19888/jobhistory/tasks/$job_id/m/ > .web.log 2>&1`;
`grep "200 OK" .web.log > /dev/null 2>&1`;
if ($? != 0) {
    `rm -rf 9.40.201.182:19888`;
    exit 1;
}
`mkdir $tgt_folder/$job_id`;
if ($? != 0) {
    `rm -rf 9.40.201.182:19888`;
    exit 1;
}
`mv 9.40.201.182:19888 $tgt_folder/$job_id/map > /dev/null 2>&1`;

`wget -r -l2 -R hadoop-root-namenode-namenode.log,hadoop-root-secondarynamenode-namenode.log,mapred-root-historyserver-opperfx10.aus.stglabs.ibm.com.log,yarn-root-resourcemanager-namenode.log,mapred-root-historyserver-namenode.log,mapred-root-historyserver-master.log,job_*_* http://9.40.201.182:19888/jobhistory/tasks/$job_id/r/ > .web.log 2>&1`;
`mv 9.40.201.182:19888 $tgt_folder/$job_id/reduce > /dev/null 2>&1`;

exit 0;
