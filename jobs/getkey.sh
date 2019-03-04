#!/bin/bash

nohup tailfile_select.pl $1 5 >> "$1.keys" &
