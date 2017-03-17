#!/usr/bin/perl
use warnings;
use strict;

if ($#ARGV != 3) {
    print "Usage: ./wget_mapreduce_job_history.pl <MASTER_IP> <MR_HISTORY_PORT> <JOB_ID> <FOLDER>\n";
    exit 1;
}

my $master_ip = $ARGV[0];
my $mr_history_port = $ARGV[1];
my $job_id = $ARGV[2];
my $tgt_folder = $ARGV[3];
`rm -rf $master_ip:$mr_history_port`;
`wget -r -l2 -R fairscheduler,*.out,*.out.*,*.log,*.log.*,SecurityAuth-test.audit,SecurityAuth-test.audit.*,userlogs,job_*_* http://$master_ip:$mr_history_port/jobhistory/tasks/$job_id/m/ > .web.log 2>&1`;
`grep "200 OK" .web.log > /dev/null 2>&1`;
if ($? != 0) {
    `rm -rf $master_ip:$mr_history_port`;
    exit 1;
}
`mkdir $tgt_folder/$job_id`;
if ($? != 0) {
    `rm -rf $master_ip:$mr_history_port`;
    exit 1;
}
`mv $master_ip:$mr_history_port $tgt_folder/$job_id/map > /dev/null 2>&1`;

`wget -r -l2 -R fairscheduler,*.out,*.out.*,*.log,*.log.*,SecurityAuth-test.audit,SecurityAuth-test.audit.*,userlogs,job_*_* http://$master_ip:$mr_history_port/jobhistory/tasks/$job_id/r/ > .web.log 2>&1`;
`mv $master_ip:$mr_history_port $tgt_folder/$job_id/reduce > /dev/null 2>&1`;

exit 0;
