#!/usr/bin/perl
use strict;
use warnings;

if ($#ARGV != 7) {
    print "Usage: ./query_yarn_app_id.pl <FINISHED_ID> <FAILED_ID> <KILLED_ID> <INFO_FILE> <TAG> <ITER> <HADOOP_HOME> <SCRIPT_PATH>\n";
    exit 1;
}

my $finished_id = $ARGV[0];
my $failed_id = $ARGV[1];
my $killed_id = $ARGV[2];
my $info_fn = $ARGV[3];
my $tag = $ARGV[4];
my $iter = $ARGV[5];
my $hadoop_home = $ARGV[6];
my $script_path = $ARGV[7];
my $app_id = "";
my $timeout = 0;

while (1) {
    $app_id = `$script_path/query_last_yarn_app_id_in_some_state.pl $hadoop_home RUNNING`;
    if ($app_id ne "NONE") {
        last;
    }
    $app_id = `$script_path/query_last_yarn_app_id_in_some_state.pl $hadoop_home FINISHED`;
    if ($app_id ne $finished_id) {
        last;
    }
    $app_id = `$script_path/query_last_yarn_app_id_in_some_state.pl $hadoop_home FAILED`;
    if ($app_id ne $failed_id) {
        last;
    }
    $app_id = `$script_path/query_last_yarn_app_id_in_some_state.pl $hadoop_home KILLED`;
    if ($app_id ne $killed_id) {
        last;
    }
    `sleep 2`;
    $timeout = $timeout + 1;
    if ($timeout >= 40) {
        `echo "TAG:$tag ITER:$iter APPID:TIMEOUT" >> $info_fn`;
        exit 1;
    }
}

`echo "TAG:$tag ITER:$iter APPID:$app_id" >> $info_fn`;
exit 0;
