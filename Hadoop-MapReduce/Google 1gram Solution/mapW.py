#!/usr/bin/env python

import sys 
import string

for line in sys.stdin:
  words = line.strip().lower().translate(None,string.punctuation).split()
  year=words[1]
  print '%s\t%s' % (year,1)
