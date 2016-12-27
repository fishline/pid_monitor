#!/usr/bin/perl
use strict;
use warnings;

if ($#ARGV < 5) {
    print "Usage: ./query_yarn_app_id.pl <APPID_FILE> <INFO_FILE> <TAG> <ITER> <HADOOP_HOME> <SCRIPT_PATH>\n";
    exit 1;
}

my $appid_fn = $ARGV[0];
my $info_fn = $ARGV[1];
my $tag = $ARGV[2];
my $iter = $ARGV[3];
my $hadoop_home = $ARGV[4];
my $script_path = $ARGV[5];
my $app_id = "";
my $timeout = 0;
my $debug_fn = "";
if ($#ARGV > 5) {
    $debug_fn = $ARGV[6];
}

# Load application ids
my %failed = ();
my %killed = ();
my %finished = ();
my $ref;
open APP_FN, "< $appid_fn" or die "Cannot open $appid_fn for read!";
while (<APP_FN>) {
    my $line = $_;
    chomp($line);
    if ($line =~ /FINISHED/) {
        $ref = \%finished;
    } elsif ($line =~ /FAILED/) {
        $ref = \%failed;
    } elsif ($line =~ /KILLED/) {
        $ref = \%killed;
    } elsif ($line ne "NONE") {
        $ref->{$line} = 1;
    }
}
close APP_FN;

my $str = "";
while (1) {
    $app_id = `$script_path/query_yarn_app_id_in_some_state.pl $hadoop_home RUNNING`;
    if ($app_id ne "NONE\n") {
        chomp($app_id);
        if (($debug_fn ne "") and ($app_id =~ /\s+/)) {
            open DEBUG, ">> $debug_fn" or die "Cannot open file $debug_fn for append";
            print DEBUG "query_yarn_app_id.pl $appid_fn $info_fn $tag $iter $hadoop_home $script_path\n";
            print DEBUG "Found more than one running task:\n";
            print DEBUG $app_id."\n\n";
            close DEBUG;
        }
        last;
    }
    $str = `$script_path/query_yarn_app_id_in_some_state.pl $hadoop_home FINISHED`;
    if ($str ne "NONE\n") {
        my @apps = split(/\n/, $str);
        my $new = 0;
        foreach my $id (@apps) {
            chomp($id);
            if (not exists $finished{$id}) {
                $new = $new + 1;
                $app_id = $id;
            }
        }
        if ($new > 0) {
            if (($debug_fn ne "") and ($new > 1)) {
                open DEBUG, ">> $debug_fn" or die "Cannot open file $debug_fn for append";
                print DEBUG "query_yarn_app_id.pl $appid_fn $info_fn $tag $iter $hadoop_home $script_path\n";
                print DEBUG "Found more than one new task in FINISHED:\n";
                print DEBUG $str."\n\n";
                close DEBUG;
            }
            last;
        }
    }
    $str = `$script_path/query_yarn_app_id_in_some_state.pl $hadoop_home FAILED`;
    if ($str ne "NONE\n") {
        my @apps = split(/\n/, $str);
        my $new = 0;
        foreach my $id (@apps) {
            chomp($id);
            if (not exists $failed{$id}) {
                $new = $new + 1;
                $app_id = $id;
            }
        }
        if ($new > 0) {
            if (($debug_fn ne "") and ($new > 1)) {
                open DEBUG, ">> $debug_fn" or die "Cannot open file $debug_fn for append";
                print DEBUG "query_yarn_app_id.pl $appid_fn $info_fn $tag $iter $hadoop_home $script_path\n";
                print DEBUG "Found more than one new task in FAILED:\n";
                print DEBUG $str."\n\n";
                close DEBUG;
            }
            last;
        }
    }
    $str = `$script_path/query_yarn_app_id_in_some_state.pl $hadoop_home KILLED`;
    if ($str ne "NONE\n") {
        my @apps = split(/\n/, $str);
        my $new = 0;
        foreach my $id (@apps) {
            chomp($id);
            if (not exists $killed{$id}) {
                $new = $new + 1;
                $app_id = $id;
            }
        }
        if ($new > 0) {
            if (($debug_fn ne "") and ($new > 1)) {
                open DEBUG, ">> $debug_fn" or die "Cannot open file $debug_fn for append";
                print DEBUG "query_yarn_app_id.pl $appid_fn $info_fn $tag $iter $hadoop_home $script_path\n";
                print DEBUG "Found more than one new task in KILLED:\n";
                print DEBUG $str."\n\n";
                close DEBUG;
            }
            last;
        }
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
