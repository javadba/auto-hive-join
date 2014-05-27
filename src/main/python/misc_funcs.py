#!/usr/bin/python
#
# misc-funcs.py:  time, collections, and parser related helper methods
# author stephenb
#
from logger import *

# Do hash replacement / string interpolation of a heredoc
def interpolate(msg, inhash=locals()):
  import StringIO

  outstr = StringIO.StringIO()
  # print str(inhash)
  print >> outstr, msg % (inhash)
  return outstr.getvalue()

def curtime_str():
  import datetime

  return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
def now():
  import datetime
  return datetime.datetime.now()

# Flattens and prints a generator/array. Generators are lazy so they do not by default print contents
def printarr(arr):
  return arr if not is_iterable(arr) else "[" + (
    ",".join(printarr(arrx) if is_iterable(arrx) else str(arrx) for arrx in arr)) + "]"

# Seconds_since: trivial helper method for execution duration in seconds
def seconds_since(start) :
  return (now() - start).seconds

# run_hive_step:  Runs a hive string by shelling out to the hive executable
def format_now():
  import datetime
  return datetime.datetime.now().strftime("%Y%m%dT%H%M%S")

# Is this a list/tuple/array but not a string?
def is_iterable(obj):
  return (not hasattr(obj, "strip") and
          (hasattr(obj, "__getitem__") or
    hasattr(obj, "__iter__")))

# Is this a python function?
def is_func(obj):
  return hasattr(obj, '__call__')

# Prettify the heredocs by stripping margins
import re

def strip_margin(text):
  return re.sub('\n[ \t]*\|', '\n', text)

def strip_heredoc(text):
  indent = len(min(re.findall('\n[ \t]*(?=\S)', text) or ['']))
  pattern = r'\n[ \t]{%d}' % (indent - 1)
  return re.sub(pattern, '\n', text)

def singlewhite(txt):
  oldc = ""

  def lastchar(curstr, newc):
    global oldc
    # print "oldc=%s curstr=%s\n" %(oldc,curstr)
    rval = (curstr + newc) if newc not in [' ', '\t'] or oldc not in [' ', '\t'] else curstr
    oldc = newc
    return rval
  return reduce(lastchar, txt, "")
