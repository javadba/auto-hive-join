#!/usr/bin/python
#
# io_funcs.py:  IO/File related Helper methods for running the classifier workflow
# author stephenb
#
#
from logger import *

def read_props(props_file):
  props = {}
  with open(props_file, 'r') as f:
    print "reading propsfile %(props_file)s. " % (locals())
    for line in f.readlines():
      line = line.strip()
      if line:
        key, val  = line.split("=")
        props[key] = val
  return props

# Read contents of a file
def readfile(fname):
  debug("Reading file %s ..\n" % (fname))
  try:
    with open(fname, 'r') as f:
      # print "methods for f: %s" %dir(f)
      contents = '\n'.join(f.readlines())
      # debug("Wrote %d bytes to file %s" %(wlen, fname))
  except:
    tracerr("Unable to open %s" % fname,True)
    contents = ""
  return contents

# Writes data to a file and returns length of data written
def writefile(fname, contents):
  debug("Writing to file %s ..\n" % (fname))
  with open(fname, 'w') as f:
    wlen = f.write(contents)
    # debug("Wrote %d bytes to file %s" %(wlen, fname))
  return wlen

def pwd():
  import os
  dir = os.path.dirname(os.path.abspath(__file__))
  debug("pwd(): dir is %s" %dir)
  return dir

def exists(path):
  import os
  return os.path.exists(path)

def mkdirs(path):
  import os, errno
  try:
    os.makedirs(path)
  except OSError as exc: # Python >2.5
    if exc.errno == errno.EEXIST and os.path.isdir(path):
      pass
    else:
      raise
