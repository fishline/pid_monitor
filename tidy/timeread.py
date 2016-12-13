#!/usr/bin/python

'''
Input:  file
Output: stdout
Tidy output of /usr/bin/time --verbose into CSV format
Jeremy Schaub
$ ./timeread.py [time_output_file]
'''

import os
import sys
import subprocess
import measurement

class TimeMeasurement(measurement.Measurement):

    '''
    Data structure for /usr/bin/time measurement
    '''

    def __init__(self):
        #self.elapsed_time_sec = ''
        #self.user_time_sec = ''
        #self.system_time_sec = ''
        #self.exit_status = '-1'
        #self.cpu_pct = ''
        self.success_count = 0
        self.fail_count = 0
        self.count = ""
        self.min_sec = 0
        self.max_sec = 0
        self.median_sec = 0
        self.spark_event_log = "NA"
        self._expected_length = 7  # Change this when adding new fields

    def parse(self, time_fn, run_id, run_count):
        '''
        This parses the output of "/usr/bin/time --verbose"
        Parsing these fields:  exit_status, user_time_sec, elapsed_time_sec,
                               system_time_sec, cpu_percent
        '''
        run_dir = os.environ.get('RUNDIR')
        pmh = os.environ.get('PMH')
        duration = []
        if subprocess.call("ls " + run_dir + "/spark_events/*" + run_id + "-ITER* > /dev/null 2>&1", shell=True) == 0:
            files_str = subprocess.check_output("ls " + run_dir + "/spark_events/*" + run_id + "-ITER* | awk -F/ '{print $NF}'", shell=True)
            files = files_str.split()
            # Missing spark event log is considered as task failure
            if len(files) < run_count:
                self.fail_count += (run_count - len(files))
            for f in files:
                if subprocess.call(pmh + "/workload/spark/scripts/analyze_spark_event_log.sh " + run_dir + "/spark_events/" + f + " > /dev/null 2>&1", shell=True) == 0:
                    self.success_count += 1
                    duration.append(float(subprocess.check_output(pmh + "/workload/spark/scripts/analyze_spark_event_log.sh " + run_dir + "/spark_events/" + f, shell=True)))
                else:
                    self.fail_count += 1
            if len(duration) == 0:
                duration.append(0.0)
            self.min_sec = min(duration)
            self.max_sec = max(duration)
            quotient, remainder = divmod(len(duration), 2)
            if remainder:
                self.median_sec = sorted(duration)[quotient]
            else:
                self.median_sec = sum(sorted(duration)[quotient - 1:quotient + 1]) / 2

            # Get Spark event log link for this run
            event_log_fn = subprocess.check_output("ls " + run_dir + "/spark_events/*" + run_id + "-ITER* | head -n 1 | awk -F/ '{print $NF}' | awk -F\"-" + run_id + "-ITER\" '{print $1}'", shell=True)
            if subprocess.call("ps -ef | grep java | grep -i historyserver | grep -v org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer | awk '{print $2}' | xargs -i sh -c \"netstat -nap | grep {}\" | grep LISTEN | awk '{print $4}' | awk -F: '{print $NF}'", shell=True) == 0:
                port = subprocess.check_output("ps -ef | grep java | grep -i historyserver | grep -v org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer | awk '{print $2}' | xargs -i sh -c \"netstat -nap | grep {}\" | grep LISTEN | awk '{print $4}' | awk -F: '{print $NF}'", shell=True)
                if subprocess.call("ip route show | grep ^default | awk '{print $5}' | xargs -i ifconfig {} | grep netmask > /dev/null 2>&1", shell=True) == 0:
                    ip = subprocess.check_output("ip route show | grep ^default | awk '{print $5}' | xargs -i ifconfig {} | grep netmask | awk '{print $2}'", shell=True)
                    self.spark_event_log = "<a href=\"http://" + ip + ":" + port + "/history/" + event_log_fn  + "\">" + event_log_fn + "</a>"
        else:
            self.fail_count = run_count
        self.count = str(self.success_count) + "/" + str(self.fail_count)
        
        #try:
        #    with open(time_fn, 'r') as fid:
        #        blob = fid.read()
        #    self.exit_status = blob.split('Exit status: ')[1].split('\n')[0].strip()
        #    if self.exit_status != '0':
        #        sys.stderr.write(
        #            "WARNING! non-zero exit status = %s in file \n\t%s\n" %
        #            (self.exit_status, time_fn))
        #    self.user_time_sec = blob.split(
        #        'User time (seconds): ')[1].split('\n')[0].strip()
        #    self.system_time_sec = blob.split(
        #        'System time (seconds): ')[1].split('\n')[0].strip()
        #    val = blob.split('Elapsed (wall clock) time (h:mm:ss or m:ss): ')[
        #        1].split('\n')[0].strip()
        #    if len(val.split(':')) == 2:   # m:ss
        #        val = str(
        #            int(val.split(':')[0]) * 60 + float(val.split(':')[1].strip()))
        #    elif len(val.split(':')) == 3:   # h:m:ss
        #        val = str(int(val.split(':')[
        #                  0]) * 3600 + int(val.split(':')[1]) * 60 +
        #                  float(val.split(':')[2].strip()))
        #    self.elapsed_time_sec = val
        #    pmh = os.environ.get('PMH')
        #    if subprocess.call("ls " + pmh + "/workload/spark/event_logs/app-*." + run_id + " > /dev/null 2>&1", shell=True) == 0:
        #        self.elapsed_time_sec = subprocess.check_output("PMH=" + pmh + " " + pmh + "/workload/spark/scripts/analyze_spark_event_log.sh " + run_id + " | tr -d '\n'", shell=True)
        #
        #    self.cpu_pct = blob.split('Percent of CPU this job got: ')[
        #        1].split('\n')[0].strip('%')
        #except Exception as err:
        #    sys.stderr.write('Problem parsing time file %s\n%s\n' %
        #                     (time_fn, str(err)))

    def htmlclass(self):
        #return "warning" if int(self.exit_status) != 0 else ""
        return ""


def main(time_fn, run_id, run_count):
    # Wrapper to write csv to stdout
    m = TimeMeasurement()
    m.parse(time_fn, run_id, run_count)
    sys.stdout.write('%s\n%s\n' % (m.headercsv(), m.rowcsv()))


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3])
