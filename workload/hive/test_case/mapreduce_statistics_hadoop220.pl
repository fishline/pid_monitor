#!/usr/bin/perl
use warnings;
use strict;

if ($#ARGV != 0) {
    print "Usage: ./mapreduce_statistics_hadoop220.pl <job history folder>\n";
    exit 1;
}

my $job_folder = $ARGV[0];
my @folder_fields = split(/\//, $job_folder);
my $job_name = $folder_fields[$#folder_fields];
chomp($job_name);
my %host_map_count = ();
my %host_map_ms_total = ();
my %host_map_CPU_time_total = ();
my %map_host = ();
my $queue = `grep $job_name $job_folder/map/jobhistory/app | awk -F\\\" '{print \$12}'`;
chomp($queue);
opendir (DIR, "$job_folder/map/jobhistory/task/") or die $!;
while (my $file = readdir(DIR)) {
    if ($file ne "." and $file ne "..") {
        if ($file =~ /_m_/) {
            my $host = `grep -A 1 "var attemptsTableData" $job_folder/map/jobhistory/task/$file | tail -n 1 | awk -F, '{print \$3}' | awk -F"default-rack/" '{print \$2}' | awk -F: '{print \$1}'`;
            chomp($host);
            my $ms = `grep -A 1 "var attemptsTableData" $job_folder/map/jobhistory/task/$file | tail -n 1 | awk -F, '{print \$7}' | awk -F\\" '{print \$2}'`;
            chomp($ms);
            $map_host{$file} = $host;
            if (exists $host_map_count{$host}) {
                $host_map_count{$host} = $host_map_count{$host} + 1;
                $host_map_ms_total{$host} = $host_map_ms_total{$host} + $ms;
            } else {
                $host_map_count{$host} = 1;
                $host_map_ms_total{$host} = $ms;
                $host_map_CPU_time_total{$host} = 0;
            }
        }
    }
}
closedir DIR;

opendir (DIR, "$job_folder/map/jobhistory/taskcounters/") or die $!;
while (my $file = readdir(DIR)) {
    if ($file ne "." and $file ne "..") {
        if ($file =~ /_m_/) {
            my $cpu_ms = `grep -A 2 "CPU time spent" $job_folder/map/jobhistory/taskcounters/$file | tail -n 1 | awk '{print \$1}'`;
            chomp($cpu_ms);
            $host_map_CPU_time_total{$map_host{$file}} = $host_map_CPU_time_total{$map_host{$file}} + $cpu_ms;
        }
    }
}
closedir DIR;

foreach my $key (keys %host_map_count) {
    print "HOST:$key MAP_COUNT:$host_map_count{$key} JOB_FOLDER:$job_folder QUEUE:$queue\n";
    print "HOST:$key TOTAL_MAP_DURATION:$host_map_ms_total{$key} JOB_FOLDER:$job_folder QUEUE:$queue\n";
    print "HOST:$key TOTAL_MAP_CPU_TIME:$host_map_CPU_time_total{$key} JOB_FOLDER:$job_folder QUEUE:$queue\n";
}

my %host_red_count = ();
my %red_host = ();
opendir (DIR, "$job_folder/reduce/jobhistory/task/") or die $!;
while (my $file = readdir(DIR)) {
    if ($file ne "." and $file ne "..") {
        if ($file =~ /_r_/) {
            my $host = `grep -A 1 "var attemptsTableData" $job_folder/reduce/jobhistory/task/$file | tail -n 1 | awk -F, '{print \$3}' | awk -F"default-rack/" '{print \$2}' | awk -F: '{print \$1}'`;
            chomp($host);
            $red_host{$file} = $host;
            if (exists $host_red_count{$host}) {
                $host_red_count{$host} = $host_red_count{$host} + 1;
            } else {
                $host_red_count{$host} = 1;
            }
        }
    }
}
closedir DIR;
foreach my $key (keys %host_red_count) {
    print "HOST:$key RED_COUNT:$host_red_count{$key} JOB_FOLDER:$job_folder QUEUE:$queue\n";
}

my %map_hive_records_in = ();
my %host_hive_records_in = ();
my %map_hdfs_bytes_read = ();
my %host_hdfs_bytes_read = ();
opendir (DIR, "$job_folder/map/jobhistory/taskcounters/") or die $!;
while (my $file = readdir(DIR)) {
    if ($file ne "." and $file ne "..") {
        if ($file =~ /_m_/) {
            my $records_in = `grep -A 2 "RECORDS_IN" $job_folder/map/jobhistory/taskcounters/$file | tail -n 1 | awk '{print \$1}'`;
            chomp($records_in);
            $map_hive_records_in{$file} = $records_in;
            if (exists $host_hive_records_in{$map_host{$file}}) {
                $host_hive_records_in{$map_host{$file}} = $host_hive_records_in{$map_host{$file}} + $records_in;
            } else {
                $host_hive_records_in{$map_host{$file}} = $records_in;
            }
            my $bytes_read = `grep -A 2 "HDFS: Number of bytes read" $job_folder/map/jobhistory/taskcounters/$file | tail -n 1 | awk '{print \$1}'`;
            chomp($bytes_read);
            $map_hdfs_bytes_read{$file} = $bytes_read;
            if (exists $host_hdfs_bytes_read{$map_host{$file}}) {
                $host_hdfs_bytes_read{$map_host{$file}} = $host_hdfs_bytes_read{$map_host{$file}} + $bytes_read;
            } else {
                $host_hdfs_bytes_read{$map_host{$file}} = $bytes_read;
            }
        }
    }
}
closedir DIR;
foreach my $key (keys %host_hive_records_in) {
    print "HOST:$key MAP_HIVE_RECORDS_IN:$host_hive_records_in{$key} JOB_FOLDER:$job_folder QUEUE:$queue\n";
}
foreach my $key (keys %host_hdfs_bytes_read) {
    print "HOST:$key MAP_HDFS_BYTES_READ:$host_hdfs_bytes_read{$key} JOB_FOLDER:$job_folder QUEUE:$queue\n";
}
exit 0;
