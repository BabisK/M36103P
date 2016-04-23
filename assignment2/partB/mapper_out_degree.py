#!/usr/bin/env python
import sys
 
for line in sys.stdin:
    line = line.strip()
    key = line.split(' ')
    key_value = key[0]
    print >>sys.stdout, "%s\t%s" % (key_value, 1)