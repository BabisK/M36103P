#!/usr/bin/env python

import sys

total = 0
prev_key = False

# input comes from STDIN
for line in sys.stdin:
    line = line.strip()
    data = line.split('\t')
    curr_key = data[0]
    count = int(data[1])
	
    if prev_key and curr_key != prev_key:
        print >>sys.stdout, "%s\t%i" % (prev_key, total)
        prev_key = curr_key
        total = count
    #same key; accumulate sum
    else:
        prev_key = curr_key
        total += count

# emit last key
if prev_key:
    print >>sys.stdout, "%s\t%i" % (prev_key, total)