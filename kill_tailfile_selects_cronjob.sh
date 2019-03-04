#!/bin/sh

# usage: kill_tailfile_selects.sh yyy-mm-dd
# kills the tailfile_select.pl's that are tailing a log file named yyyy-mm-dd*

yyyy_mm_dd=$(date --date='2 days ago' +%Y-%m-%d)
ps aux | grep tailfile_select.pl | grep $yyyy_mm_dd | awk '{ print $2 }' | xargs kill
