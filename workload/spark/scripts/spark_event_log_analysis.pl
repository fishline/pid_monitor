#!/usr/bin/perl
use strict;
use warnings;

if ($#ARGV != 1) {
    print "Usage: ./spark_event_log_analysis.pl <INFO_FILE> <TAG>\n";
    exit 1;
}

my $info_fn = $ARGV[0];
my $tag = $ARGV[1];
my $debug_fn = $info_fn;
$debug_fn =~ s/info$/debug.log/;

my $app_count = `grep "TAG:$tag " $info_fn | grep EVENTLOG | wc -l`;
chomp($app_count);
if ($app_count == 0) {
    open DEBUG, ">> $debug_fn" or die "Cannot open file $debug_fn for append";
    print DEBUG "spark_event_log_analysis.pl $info_fn $tag\n";
    print DEBUG "Cannot get valid spark event log for analysis\n\n";
    close DEBUG;
    print "NA\n";
    exit 1;
}

my $event_log = `grep "TAG:$tag " $info_fn | grep EVENTLOG | head -n 1 | awk '{print \$3}' | awk -F: '{print \$2}'`;
chomp($event_log);
`$ENV{'HADOOP_HOME'}/bin/hadoop fs -mkdir -p /tmp/pmh_spark_analyze > /dev/null 2>&1`;
`$ENV{'HADOOP_HOME'}/bin/hadoop fs -copyFromLocal $event_log /tmp/pmh_spark_analyze/ > /dev/null 2>&1`;
my $master_ip = `ping $ENV{'MASTER'} -c 1 | head -n 1 | awk -F\\( '{print \$2}' | awk -F\\) '{print \$1}'`;
chomp($master_ip);
`$ENV{'SPARK_HOME'}/bin/spark-submit --master yarn --num-executors 8 --executor-cores 2 --executor-memory 4g --driver-memory 4g --class src.main.scala.EventLogParsing $ENV{'PMH'}/workload/spark/resources/analyze/target/scala-2.10/analyze_2.10-1.0.jar hdfs://$master_ip/tmp/pmh_spark_analyze/$event_log > /tmp/screen_output_$event_log 2>&1`;
my @info_fields = split(/\//, $info_fn);
my $log_path = $info_fn;
$log_path = s/info$/rundir/;
`cat /tmp/screen_output_$event_log | grep -v "INFO" | grep -v "WARN" | grep -v "===" | grep -v "^SLF4J" > $log_path/$info_fields[$#info_fields-1]/latest/data/raw/analyze_$event_log.txt`;
print "<a href=\"../data/raw/analyze_".$event_log.".txt\">Analyze result</a>\n";
exit 0;
