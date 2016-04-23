#!/usr/bin/env python
import sys
 
for line in sys.stdin:
    line = line.strip()
    key = line.split('\t')
    value = key[1]
    #for word in key_value:
    print >>sys.stdout, "%s\t%s" % (value, 1)
