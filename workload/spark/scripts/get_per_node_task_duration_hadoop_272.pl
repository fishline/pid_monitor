#!/usr/bin/perl
use warnings;
use strict;

if ($#ARGV != 0) {
    print "Usage: ./get_per_node_task_duration.pl <JOB_ID>\n";
    exit 1;
}

my $job_id = $ARGV[0];
my %task_count_map = ();
my %task_accumulate_map = ();
`rm -rf 9.40.201.122:19888`;
`wget -r -l1 http://9.40.201.122:19888/jobhistory/tasks/$job_id/m/ > /dev/null 2>&1`;
opendir (DIR, "9.40.201.122\:19888/jobhistory/task/") or die $!;
while (my $file = readdir(DIR)) {
    if ($file ne "." and $file ne "..") {
        my $host = `grep -A 1 "var attemptsTableData" 9.40.201.122\:19888/jobhistory/task/$file | tail -n 1 | awk -F, '{print \$4}' | awk -F"default-rack/" '{print \$2}' | awk -F: '{print \$1}'`;
        chomp($host);
        my $ms = `grep -A 1 "var attemptsTableData" 9.40.201.122\:19888/jobhistory/task/$file | tail -n 1 | awk -F, '{print \$8}' | awk -F\\" '{print \$2}'`;
        chomp($ms);
        if (exists $task_count_map{$host}) {
            $task_count_map{$host} = $task_count_map{$host} + 1;
            $task_accumulate_map{$host} = $task_accumulate_map{$host} + $ms;
        } else {
            $task_count_map{$host} = 1;
            $task_accumulate_map{$host} = $ms;
        }
    }
}
closedir DIR;

my %task_count_reduce = ();
my %task_accumulate_reduce = ();
`rm -rf 9.40.201.122:19888`;
`wget -r -l1 http://9.40.201.122:19888/jobhistory/tasks/$job_id/r/ > /dev/null 2>&1`;
if (-e "9.40.201.122\:19888/jobhistory/task" and -d "9.40.201.122\:19888/jobhistory/task") {
    opendir (DIR, "9.40.201.122\:19888/jobhistory/task/") or die $!;
    while (my $file = readdir(DIR)) {
        if ($file ne "." and $file ne "..") {
            my $host = `grep -A 1 "var attemptsTableData" 9.40.201.122\:19888/jobhistory/task/$file | tail -n 1 | awk -F, '{print \$4}' | awk -F"default-rack/" '{print \$2}' | awk -F: '{print \$1}'`;
            chomp($host);
            my $ms = `grep -A 1 "var attemptsTableData" 9.40.201.122\:19888/jobhistory/task/$file | tail -n 1 | awk -F, '{print \$13}' | awk -F\\" '{print \$2}'`;
            chomp($ms);
            if (exists $task_count_reduce{$host}) {
                $task_count_reduce{$host} = $task_count_reduce{$host} + 1;
                $task_accumulate_reduce{$host} = $task_accumulate_reduce{$host} + $ms;
            } else {
                $task_count_reduce{$host} = 1;
                $task_accumulate_reduce{$host} = $ms;
            }
        }
    }
    closedir DIR;
    foreach my $key (keys %task_count_map) {
        my $map_avg = sprintf("%.1f", $task_accumulate_map{$key} / ($task_count_map{$key} * 1000));
        my $reduce_avg = 0;
        my $reduce_count = 0;
        if (exists $task_count_reduce{$key}) {
            $reduce_avg = sprintf("%.1f", $task_accumulate_reduce{$key} / ($task_count_reduce{$key} * 1000));
            $reduce_count = $task_count_reduce{$key};
        }
        print "$key: ($task_count_map{$key} map)$map_avg"."s, ($reduce_count reduce)$reduce_avg"."s\n";
    }
} else {
    foreach my $key (keys %task_count_map) {
        my $map_avg = sprintf("%.1f", $task_accumulate_map{$key} / ($task_count_map{$key} * 1000));
        print "$key: ($task_count_map{$key} map)$map_avg"."s\n";
    }
}

