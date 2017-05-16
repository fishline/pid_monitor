#!/usr/bin/perl
use strict;
use warnings;
use lib qw(..);
use JSON qw( );
use File::Basename;

if ($#ARGV + 1 != 5) {
    die "Usage: ./sweeping_spark.pl <test working dir> <test plan JSON> <sweeping tag> <rundir> <PMH>";
}

my $test_work_dir = $ARGV[0];
my $test_plan_json = $ARGV[1];
my $sweeping_tag = $ARGV[2];
my $rundir = $ARGV[3];
my $PMH = $ARGV[4];
$ENV{'PMH'} = $PMH;

my $scenario_text = do {
    open(my $json_fh, "<:encoding(UTF-8)", $test_work_dir."/".$test_plan_json) or die "Cannot open $test_work_dir/$test_plan_json for read!";
    local $/;
    <$json_fh>
};
my $json = JSON->new;
my $scenario = $json->decode($scenario_text);

open FD, "> $test_work_dir/spark_sweeping-$sweeping_tag.log" or die "Cannot open $test_work_dir/spark_sweeping-$sweeping_tag.log for write!";
my %config = ();
my %config_len = ();
my %current_config = ();
my %config_path = ();
my %test_results = ();
foreach my $step (@{$scenario}) {
    # For SWEEPING, create new run.sh for scheduler
    if ((exists $step->{"SWEEPING"}) and ($step->{"SWEEPING"} eq $sweeping_tag)) {
        my $query_cmd_template = "";
        foreach my $cmd_field (@{$step->{"SWEEPING_METRIC"}}) {
            $query_cmd_template = $query_cmd_template.$cmd_field." ";
        }
        # Substitute fields
        if ($query_cmd_template =~ /\<PMH\>/) {
            $query_cmd_template =~ s/\<PMH\>/$PMH/;
        }
        if ($query_cmd_template =~ /\<INFO\>/) {
            $query_cmd_template =~ s/\<INFO\>/$test_work_dir\/info/;
        }

        foreach my $key (keys %{$step->{"SWEEPING_PARAMS"}}) {
            $config{$key} = $step->{"SWEEPING_PARAMS"}->{$key};
            $config_len{$key} = scalar @{$config{$key}};
        }
        # Choose random start point
        foreach my $key (keys %{$step->{"SWEEPING_PARAMS"}}) {
            $current_config{$key} = int(rand($config_len{$key}));
        }

        while (1) {
            # Record current configuration
            my $cmd = "";
            my $query_tag = $sweeping_tag;
            print FD "Baseline configuration: ";
            foreach my $key (keys %{$step->{"SWEEPING_PARAMS"}}) {
                $cmd = $cmd." ".$config{$key}[$current_config{$key}];
                $query_tag = $query_tag."-".$config{$key}[$current_config{$key}];
                print FD $key.$config{$key}[$current_config{$key}]."(".$current_config{$key}.") ";
            }
            print FD "\n";
            my $baseline_result = 0;
            my $query_cmd = "";
            if (not exists $test_results{$cmd}) {
                # Get a baseline run
                print FD "Run command for baseline: $test_work_dir/_run_$sweeping_tag.sh $rundir $cmd\n";
                `$test_work_dir/_run_$sweeping_tag.sh $rundir $cmd`;
                $query_cmd = $query_cmd_template;
                if ($query_cmd =~ /\<TAG\>/) {
                    $query_cmd =~ s/\<TAG\>/$query_tag/;
                }
                print FD "Query cmd: $query_cmd\n";
                $baseline_result = `$query_cmd`;
                chomp($baseline_result);
                $test_results{$cmd} = $baseline_result;
            } else {
                $baseline_result = $test_results{$cmd};
            }
            print FD "Got baseline result: $baseline_result\n";
            if ($baseline_result eq "NA") {
                print FD "Baseline failed, exiting...\n";
                last;
            }

            # For each argument, try move one step in each direction (or single direction in boundary conditions)
            # compare with baseline to see which configuration gives best result
            my %new_config = ();
            foreach my $key (keys %{$step->{"SWEEPING_PARAMS"}}) {
                $new_config{$key} = $current_config{$key};
                print FD "Try $key ";
                if ($current_config{$key} == 0) {
                    # Try +1
                    print FD "+1\n";
                    $cmd = "";
                    $query_tag = $sweeping_tag;
                    foreach my $elem_key (keys %{$step->{"SWEEPING_PARAMS"}}) {
                        if ($elem_key eq $key) {
                            $cmd = $cmd." ".$config{$key}[1];
                            $query_tag = $query_tag."-".$config{$key}[1];
                        } else {
                            $cmd = $cmd." ".$config{$elem_key}[$current_config{$elem_key}];
                            $query_tag = $query_tag."-".$config{$elem_key}[$current_config{$elem_key}];
                        }
                    }
                    my $trial_result = 0;
                    if (not exists $test_results{$cmd}) {
                        print FD "Run command for trial: $test_work_dir/_run_$sweeping_tag.sh $rundir $cmd\n";
                        `$test_work_dir/_run_$sweeping_tag.sh $rundir $cmd`;

                        $query_cmd = $query_cmd_template;
                        if ($query_cmd =~ /\<TAG\>/) {
                            $query_cmd =~ s/\<TAG\>/$query_tag/;
                        }
                        $trial_result = `$query_cmd`;
                        chomp($trial_result);
                        $test_results{$cmd} = $trial_result;
                    } else {
                        $trial_result = $test_results{$cmd};
                    }
                    print FD "Got trial result for $cmd: $trial_result\n";
                    
                    if (($trial_result ne "NA") and ($trial_result < $baseline_result)) {
                        print FD "Trial +1 result better, update new_config\n";
                        $new_config{$key} = 1;
                    }
                } elsif ($current_config{$key} == ($config_len{$key} - 1)) {
                    # Try -1
                    print FD "-1\n";
                    $cmd = "";
                    $query_tag = $sweeping_tag;
                    foreach my $elem_key (keys %{$step->{"SWEEPING_PARAMS"}}) {
                        if ($elem_key eq $key) {
                            $cmd = $cmd." ".$config{$key}[$config_len{$key} - 2];
                            $query_tag = $query_tag."-".$config{$key}[$config_len{$key} -2];
                        } else {
                            $cmd = $cmd." ".$config{$elem_key}[$current_config{$elem_key}];
                            $query_tag = $query_tag."-".$config{$elem_key}[$current_config{$elem_key}];
                        }
                    }
                    my $trial_result = 0;
                    if (not exists $test_results{$cmd}) {
                        print FD "Run command for trial: $test_work_dir/_run_$sweeping_tag.sh $rundir $cmd\n";
                        `$test_work_dir/_run_$sweeping_tag.sh $rundir $cmd`;

                        $query_cmd = $query_cmd_template;
                        if ($query_cmd =~ /\<TAG\>/) {
                            $query_cmd =~ s/\<TAG\>/$query_tag/;
                        }
                        $trial_result = `$query_cmd`;
                        chomp($trial_result);
                        $test_results{$cmd} = $trial_result;
                    } else {
                        $trial_result = $test_results{$cmd};
                    }
                    print FD "Got trial result for $cmd: $trial_result\n";
                    
                    if (($trial_result ne "NA") and ($trial_result < $baseline_result)) {
                        print FD "Trial -1 result better, update new_config\n";
                        $new_config{$key} = $config_len{$key} - 2;
                    }

                } else {
                    # Try both directions
                    print FD "+1/-1\n";
                    $cmd = "";
                    $query_tag = $sweeping_tag;
                    foreach my $elem_key (keys %{$step->{"SWEEPING_PARAMS"}}) {
                        if ($elem_key eq $key) {
                            $cmd = $cmd." ".$config{$key}[$current_config{$key} + 1];
                            $query_tag = $query_tag."-".$config{$key}[$current_config{$key} + 1];
                        } else {
                            $cmd = $cmd." ".$config{$elem_key}[$current_config{$elem_key}];
                            $query_tag = $query_tag."-".$config{$elem_key}[$current_config{$elem_key}];
                        }
                    }
                    my $trial_result_plus = 0;
                    if (not exists $test_results{$cmd}) {
                        print FD "Run command for trial +1: $test_work_dir/_run_$sweeping_tag.sh $rundir $cmd\n";
                        `$test_work_dir/_run_$sweeping_tag.sh $rundir $cmd`;

                        $query_cmd = $query_cmd_template;
                        if ($query_cmd =~ /\<TAG\>/) {
                            $query_cmd =~ s/\<TAG\>/$query_tag/;
                        }
                        $trial_result_plus = `$query_cmd`;
                        chomp($trial_result_plus);
                        $test_results{$cmd} = $trial_result_plus;
                    } else {
                        $trial_result_plus = $test_results{$cmd};
                    }
                    print FD "Got trial +1 result for $cmd: $trial_result_plus\n";
                    
                    $cmd = "";
                    $query_tag = $sweeping_tag;
                    foreach my $elem_key (keys %{$step->{"SWEEPING_PARAMS"}}) {
                        if ($elem_key eq $key) {
                            $cmd = $cmd." ".$config{$key}[$current_config{$key} - 1];
                            $query_tag = $query_tag."-".$config{$key}[$current_config{$key} - 1];
                        } else {
                            $cmd = $cmd." ".$config{$elem_key}[$current_config{$elem_key}];
                            $query_tag = $query_tag."-".$config{$elem_key}[$current_config{$elem_key}];
                        }
                    }
                    my $trial_result_minus = 0;
                    if (not exists $test_results{$cmd}) {
                        print FD "Run command for trial -1: $test_work_dir/_run_$sweeping_tag.sh $rundir $cmd\n";
                        `$test_work_dir/_run_$sweeping_tag.sh $rundir $cmd`;

                        $query_cmd = $query_cmd_template;
                        if ($query_cmd =~ /\<TAG\>/) {
                            $query_cmd =~ s/\<TAG\>/$query_tag/;
                        }
                        $trial_result_minus = `$query_cmd`;
                        chomp($trial_result_minus);
                        $test_results{$cmd} = $trial_result_minus;
                    } else {
                        $trial_result_minus = $test_results{$cmd};
                    }
                    print FD "Got trial -1 result for $cmd: $trial_result_minus\n";
 
                    if ($trial_result_plus eq "NA") {
                        if (($trial_result_minus ne "NA") and ($trial_result_minus < $baseline_result)) {
                            print FD "Trial -1 result better, update new_config\n";
                            $new_config{$key} = $current_config{$key} - 1;
                        }
                    } elsif ($trial_result_minus eq "NA") {
                        if ($trial_result_plus < $trial_result_minus) {
                            print FD "Trial +1 result better, update new_config\n";
                            $new_config{$key} = $current_config{$key} + 1;
                        }
                    } else {
                        if (($trial_result_plus < $baseline_result) or ($trial_result_minus < $baseline_result)) {
                            if ($trial_result_plus < $trial_result_minus) {
                                print FD "Trial +1 result better, update new_config\n";
                                $new_config{$key} = $current_config{$key} + 1;
                            } else {
                                print FD "Trial -1 result better, update new_config\n";
                                $new_config{$key} = $current_config{$key} - 1;
                            }
                        }
                    }
                }
            }

            # Choose the best for each argument. If this is the same as baseline, we have converged to optimum, otherwise this is new baseline
            my $diff = 0;
            foreach my $key (keys %{$step->{"SWEEPING_PARAMS"}}) {
                if ($current_config{$key} ne $new_config{$key}) {
                    $diff = 1;
                }
            }
            if ($diff == 0) {
                print FD "Result converged\n";
                last;
            } else {
                print FD "Update baseline configuration\n";
                foreach my $key (keys %{$step->{"SWEEPING_PARAMS"}}) {
                    $current_config{$key} = $new_config{$key};
                }
            }
        }
        last;
    }
}
close FD;

