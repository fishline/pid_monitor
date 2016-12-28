echo "check CPU usage"
echo "avg-cpu:  %user   %nice %system %iowait  %steal   %idle"
clush -g datanodes "iostat -c 1 2|tail -n 2"
echo

